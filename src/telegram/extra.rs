//! Additional read-only account collections. Completeness always refers to an API snapshot.
use super::*;
use crate::query::Query;

const CATEGORIES: &[&str] = &[
    "calls",
    "payments",
    "gifts",
    "boosts",
    "business",
    "statistics",
    "locations",
    "story_viewers",
    "bots",
    "local_data",
    "scheduled",
];

pub(super) fn page_scope(scope: &str, args: &Value) -> String {
    format!(
        "{scope}/page:{}",
        blake3::hash(args.to_string().as_bytes()).to_hex()
    )
}

fn base_scope(scope: &str) -> &str {
    scope.split("/page:").next().unwrap_or(scope)
}

/// These collectors have opaque server cursors, not numeric offsets or page-length termination.
fn cursor_field(method: &str) -> &'static str {
    if method == "payments.getStarsSubscriptions" {
        "subscriptions_next_offset"
    } else {
        "next_offset"
    }
}

fn next_page(method: &str, args: &Value, value: &Value) -> Result<Option<Value>> {
    ensure!(value["inexact"] != true, "server marked the result inexact");
    ensure!(
        !value["_"].as_str().unwrap_or("").ends_with("NotModified"),
        "uncached request returned NotModified"
    );
    let mut next = args.clone();
    if method == "messages.searchGlobal" {
        let messages = value["messages"].as_array().context("missing messages")?;
        let Some(last) = messages.last() else {
            return Ok(None);
        };
        let peer = &last["peer_id"];
        let key = peer_key(peer).context("search cursor peer missing")?;
        // Resolve the input peer using the response/cache in extra_pages.
        next["offset_peer"] = json!({"key":key});
        next["offset_id"] = last["id"].clone();
        next["offset_rate"] = value
            .get("next_rate")
            .cloned()
            .unwrap_or_else(|| last["date"].clone());
    } else {
        let field = cursor_field(method);
        let Some(offset) = value.get(field).filter(|v| !v.is_null()) else {
            return Ok(None);
        };
        let offset = offset.as_str().context("non-string pagination cursor")?;
        if offset.is_empty() {
            return Ok(None);
        }
        next["offset"] = json!(offset);
    }
    Ok(Some(next))
}

pub(super) fn outside_call_bounds(options: &SyncOptions, message: &Value) -> bool {
    options
        .min_id
        .is_some_and(|id| integer(&message["id"]).is_some_and(|v| v <= id))
        || options
            .max_id
            .is_some_and(|id| integer(&message["id"]).is_some_and(|v| v >= id))
        || options
            .since
            .is_some_and(|date| integer(&message["date"]).is_some_and(|v| v < date))
        || options
            .until
            .is_some_and(|date| integer(&message["date"]).is_some_and(|v| v >= date))
}

fn error_status(error: &anyhow::Error) -> &'static str {
    let message = error.to_string();
    if [
        "CHAT_ADMIN_REQUIRED",
        "CHANNEL_PRIVATE",
        "CHAT_FORBIDDEN",
        "PREMIUM_ACCOUNT_REQUIRED",
        "USER_NOT_PARTICIPANT",
    ]
    .iter()
    .any(|code| message.contains(code))
    {
        "inaccessible"
    } else {
        "incomplete"
    }
}

impl Engine {
    fn extra_coverage(&self, scope: &str, status: &str, details: Value) -> Result<()> {
        let mut details = details;
        details["job"] = json!(self.job);
        details["scope"] = json!(scope);
        details["category"] = json!(
            scope
                .strip_prefix("extra/")
                .unwrap_or(scope)
                .split('/')
                .next()
        );
        details["completeness"] = json!("available API snapshot; not all historical activity");
        self.archive
            .lock()
            .unwrap()
            .coverage(scope, status, &details)
    }

    pub(super) fn prepare_extra_coverage(&self) -> Result<()> {
        let a = self.archive.lock().unwrap();
        for category in CATEGORIES {
            a.coverage(
                &format!("extra/{category}"),
                "not_started",
                &json!({"job":self.job,"category":category}),
            )?;
        }
        for (scope, reason) in [
            (
                "local_data",
                "Device-local settings, cache, contacts and unsynchronized drafts are not exposed by the user API",
            ),
            (
                "bots/local_storage",
                "Mini-app local storage and internal activity logs are not exposed by the user API",
            ),
            (
                "business/external_automation",
                "External business bot workflows are not exposed by the user API",
            ),
            (
                "locations/full_history",
                "No GPS history endpoint; only shared locations and observed live-location revisions",
            ),
            (
                "story_viewers/own_view_history",
                "No complete history endpoint for the stories this account viewed",
            ),
            (
                "boosts/full_history",
                "Boost endpoints expose current available data; older states require prior observations",
            ),
        ] {
            a.coverage(
                &format!("extra/{scope}"),
                "unsupported",
                &json!({"job":self.job,"reason":reason}),
            )?;
        }
        if self.takeout.is_none() {
            a.coverage("extra/local_data/saved_contacts", "requires_takeout", &json!({"job":self.job,"method":"contacts.getSaved","reason":"Run sync --takeout to retrieve uploaded contacts"}))?;
        }
        Ok(())
    }

    pub(super) fn finish_extra_coverage(&self) -> Result<()> {
        let a = self.archive.lock().unwrap();
        for category in CATEGORIES {
            if *category == "local_data" {
                continue;
            }
            let scope = format!("extra/{category}");
            let rows: Vec<(String, String)> =
                a.db.prepare("SELECT status,details FROM coverage WHERE name LIKE ?1")?
                    .query_map([format!("{scope}/%")], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<rusqlite::Result<_>>()?;
            let mut states = Vec::new();
            for (status, details) in rows {
                if serde_json::from_str::<Value>(&details)?["job"] == self.job {
                    states.push(status);
                }
            }
            let status = if states.is_empty() {
                "not_applicable"
            } else if states.iter().any(|s| {
                matches!(
                    s.as_str(),
                    "incomplete" | "inaccessible" | "not_started" | "in_progress"
                )
            }) {
                "incomplete"
            } else if states
                .iter()
                .any(|s| matches!(s.as_str(), "unsupported" | "limited" | "excluded"))
            {
                "limited"
            } else {
                "complete"
            };
            a.coverage(&scope, status, &json!({"job":self.job,"category":category,"scopes":states.len(),"completeness":"available API snapshots only; see child scopes"}))?;
        }
        Ok(())
    }

    async fn extra_collect(
        &self,
        method: &str,
        args: Value,
        scope: &str,
        dc: Option<i32>,
    ) -> Result<Option<Value>> {
        self.extra_coverage(scope, "in_progress", json!({"method":method}))?;
        let requested_at = now();
        let queue_peer = if method == "messages.getScheduledHistory" {
            if args["peer"]["_"] == "inputPeerSelf" {
                self.archive
                    .lock()
                    .unwrap()
                    .checkpoint("account_id")?
                    .and_then(|id| integer(&id))
                    .map(|id| format!("user:{id}"))
            } else {
                peer_key(&args["peer"])
            }
        } else {
            None
        };
        match self.rpc(method, args, None, dc).await {
            Ok((root, bytes, value)) => {
                let snapshot_key = format!("scheduled_snapshot:{}:{scope}", self.job);
                let snapshot = json!({"snapshot_revision":requested_at});
                self.capture(
                    &root,
                    &bytes,
                    method,
                    Some(scope),
                    None,
                    queue_peer
                        .as_ref()
                        .map(|_| (snapshot_key.as_str(), &snapshot)),
                )?;
                let incomplete_queue = matches!(
                    method,
                    "messages.getScheduledHistory" | "messages.getQuickReplyMessages"
                ) && integer(&value["count"]).is_some_and(|count| {
                    count > value["messages"].as_array().map_or(0, |v| v.len()) as i64
                });
                let filtered = self.extra_filtered_messages(method, &value)?;
                let incomplete = incomplete_queue
                    || value["inexact"] == true
                    || value["_"].as_str().unwrap_or("").ends_with("NotModified");
                if !incomplete
                    && filtered == 0
                    && let Some(peer) = queue_peer
                {
                    self.reconcile_scheduled_snapshot(&peer, &root, &bytes, &value, requested_at)?;
                }
                self.extra_coverage(
                    scope,
                    if incomplete { "incomplete" } else if filtered > 0 { "limited" } else { "complete" },
                    json!({"method":method,"dc":dc,"requested_at":requested_at,"filtered_messages":filtered,"message_count":value["messages"].as_array().map(Vec::len)}),
                )?;
                Ok(Some(value))
            }
            Err(error) => {
                self.check_stop()?;
                self.extra_coverage(
                    scope,
                    error_status(&error),
                    json!({"method":method,"error":error.to_string()}),
                )?;
                Ok(None)
            }
        }
    }

    fn reconcile_scheduled_snapshot(
        &self,
        peer: &str,
        root: &str,
        bytes: &[u8],
        value: &Value,
        requested_at: i64,
    ) -> Result<()> {
        let messages = value["messages"]
            .as_array()
            .context("scheduled snapshot missing messages")?;
        let present: HashSet<i64> = messages.iter().filter_map(|m| integer(&m["id"])).collect();
        let mut a = self.archive.lock().unwrap();
        let prefix = format!("{peer}/scheduled_message:");
        // Do not remove or supersede updates received while the RPC was in flight.
        let previous: Vec<(String, String)> = a.db.prepare("SELECT h.key,o.metadata FROM heads h JOIN observations o ON o.id=h.observation WHERE o.kind='scheduled_message' AND o.deleted=0 AND o.observed<=?1 AND substr(h.key,1,length(?2))=?2")?
            .query_map(params![requested_at,prefix], |r| Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        let mut removed = Vec::new();
        for (key, metadata) in previous {
            let mut metadata: Value = serde_json::from_str(&metadata)?;
            let Some(id) = integer(&metadata["message_id"]) else {
                continue;
            };
            if present.contains(&id) {
                continue;
            }
            metadata["revision"] = json!(requested_at);
            metadata["queue_state"] = json!("absent_from_complete_snapshot");
            removed.push(Capture {
                key,
                kind: "scheduled_message".into(),
                root_type: root.into(),
                bytes: bytes.to_vec(),
                observed_at: now(),
                source: "scheduled_snapshot".into(),
                metadata,
                replay_key: None,
                partial: false,
                deleted: true,
            });
        }
        if !removed.is_empty() {
            a.ingest(&self.schema_hash, &removed, None)?;
            a.materialize()?;
        }
        Ok(())
    }

    fn extra_filtered_messages(&self, method: &str, value: &Value) -> Result<usize> {
        if !matches!(
            method,
            "messages.searchGlobal"
                | "messages.getScheduledHistory"
                | "messages.getScheduledMessages"
                | "messages.getRecentLocations"
        ) {
            return Ok(0);
        }
        let a = self.archive.lock().unwrap();
        let selector = Selector::parse(&a.config.history_selector)?;
        let mut filtered = 0;
        for message in value["messages"].as_array().into_iter().flatten() {
            let metadata = peer_key(&message["peer_id"])
                .map(|key| peer_metadata(&a, &key))
                .transpose()?
                .flatten()
                .unwrap_or(json!({}));
            let outside_range =
                method == "messages.searchGlobal" && outside_call_bounds(&self.options, message);
            if outside_range || !selector.matches(&message_context(&metadata, message)) {
                filtered += 1;
            }
        }
        Ok(filtered)
    }

    async fn extra_pages(&self, method: &str, mut args: Value, scope: &str) -> Result<()> {
        let checkpoint = format!("extra:{}:{scope}", self.job);
        let saved = self.archive.lock().unwrap().checkpoint(&checkpoint)?;
        let mut seen: HashSet<String> = HashSet::new();
        let mut filtered = 0;
        if let Some(saved) = saved {
            filtered = integer(&saved["filtered_messages"]).unwrap_or(0) as usize;
            if saved["complete"] == true {
                if !self.options.continuous {
                    self.extra_coverage(
                        scope,
                        if filtered > 0 { "limited" } else { "complete" },
                        json!({"method":method,"resumed":true,"filtered_messages":filtered}),
                    )?;
                    return Ok(());
                }
                filtered = 0;
            } else {
                args = saved["args"].clone();
                seen = serde_json::from_value(saved.get("seen").cloned().unwrap_or(json!([])))?;
            }
        }
        loop {
            self.check_stop()?;
            let total = self
                .archive
                .lock()
                .unwrap()
                .checkpoint(&format!("messages:{}", self.job))?
                .and_then(|v| integer(&v))
                .unwrap_or(0) as u64;
            if method == "messages.searchGlobal" {
                if self.message_limit()? {
                    self.extra_coverage(
                        scope,
                        "incomplete",
                        json!({"method":method,"reason":"message limit","cursor":args}),
                    )?;
                    return Ok(());
                }
                args["limit"] = json!(
                    self.options
                        .max_messages
                        .map(|n| n
                            .saturating_sub(total.saturating_sub(self.base_messages))
                            .min(100))
                        .unwrap_or(100)
                );
            }
            self.extra_coverage(scope, "in_progress", json!({"method":method,"cursor":args}))?;
            let (root, bytes, value) = match self.rpc(method, args.clone(), None, None).await {
                Ok(response) => response,
                Err(error) => {
                    self.check_stop()?;
                    self.extra_coverage(
                        scope,
                        error_status(&error),
                        json!({"method":method,"cursor":args,"error":error.to_string()}),
                    )?;
                    return Ok(());
                }
            };
            let page = page_scope(scope, &args);
            // Cache input peers for global-search cursors; payloads and cursor commit below.
            if method == "messages.searchGlobal" {
                let archive = self.archive.lock().unwrap();
                let self_id = archive
                    .checkpoint("account_id")?
                    .and_then(|v| integer(&v))
                    .unwrap_or(0);
                for peer in value["users"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .chain(value["chats"].as_array().into_iter().flatten())
                {
                    cache_peer(&archive, peer, self_id)?;
                }
                drop(archive);
                self.refresh_folders()?;
            }
            let advance = (|| -> Result<Option<Value>> {
                let mut next = next_page(method, &args, &value)?;
                if let Some(next) = next.as_mut() {
                    if let Some(key) = next["offset_peer"]["key"].as_str() {
                        next["offset_peer"] = self.input_peer(key)?;
                    }
                    ensure!(
                        *next != args && !seen.contains(&next.to_string()),
                        "collector cursor repeated"
                    );
                }
                Ok(next)
            })();
            let next = match advance {
                Ok(next) => next,
                Err(error) => {
                    // Keep malformed/non-progressing responses, but never consume their cursor.
                    self.capture(&root, &bytes, method, Some(&page), None, None)?;
                    self.extra_coverage(
                        scope,
                        "incomplete",
                        json!({"method":method,"cursor":args,"error":error.to_string()}),
                    )?;
                    return Ok(());
                }
            };
            filtered += self.extra_filtered_messages(method, &value)?;
            seen.insert(args.to_string());
            let complete = next.is_none();
            let mut progress = json!({"args":next.as_ref().unwrap_or(&args),"seen":seen,"complete":complete,"filtered_messages":filtered});
            if method == "messages.searchGlobal" {
                progress["counter_key"] = json!(format!("messages:{}", self.job));
                progress["messages"] =
                    json!(total + value["messages"].as_array().map_or(0, |m| m.len()) as u64);
            }
            // The page, counters and consumed cursor commit in one transaction.
            self.capture(
                &root,
                &bytes,
                method,
                Some(&page),
                None,
                Some((&checkpoint, &progress)),
            )?;
            self.extra_coverage(
                scope,
                if !complete { "in_progress" } else if filtered > 0 { "limited" } else { "complete" },
                json!({"method":method,"pages":seen.len(),"cursor":args,"filtered_messages":filtered}),
            )?;
            let Some(next) = next else { return Ok(()) };
            args = next;
        }
    }

    pub(super) async fn extra_saved_contacts(&self) -> Result<()> {
        self.extra_collect(
            "contacts.getSaved",
            json!({}),
            "extra/local_data/saved_contacts",
            None,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn extra_account_collectors(&self) -> Result<()> {
        self.extra_collect(
            "payments.getSavedInfo",
            json!({}),
            "extra/payments/saved_info",
            None,
        )
        .await?;
        self.extra_collect("premium.getMyBoosts", json!({}), "extra/boosts/mine", None)
            .await?;
        self.extra_collect(
            "account.getConnectedBots",
            json!({}),
            "extra/business/connected_bots",
            None,
        )
        .await?;
        self.extra_collect(
            "account.getBusinessChatLinks",
            json!({}),
            "extra/business/chat_links",
            None,
        )
        .await?;
        self.extra_collect(
            "messages.getAttachMenuBots",
            json!({"hash":"0"}),
            "extra/bots/attachment_menu",
            None,
        )
        .await?;
        self.extra_pages("messages.searchGlobal", json!({"q":"","filter":{"_":"inputMessagesFilterPhoneCalls"},"min_date":self.options.since.map(|date| date.saturating_sub(1)).unwrap_or(0),"max_date":self.options.until.unwrap_or(0),"offset_rate":0,"offset_peer":{"_":"inputPeerEmpty"},"offset_id":0,"limit":100}), "extra/calls/history").await?;
        self.extra_finances("self", &json!({"_":"inputPeerSelf"}))
            .await?;
        self.extra_gifts("self", &json!({"_":"inputPeerSelf"}))
            .await?;
        Ok(())
    }

    async fn extra_finances(&self, key: &str, peer: &Value) -> Result<()> {
        self.extra_collect(
            "payments.getStarsStatus",
            json!({"peer":peer}),
            &format!("extra/payments/{key}/balance"),
            None,
        )
        .await?;
        self.extra_pages(
            "payments.getStarsTransactions",
            json!({"peer":peer,"offset":"","limit":100}),
            &format!("extra/payments/{key}/transactions"),
        )
        .await?;
        self.extra_pages(
            "payments.getStarsSubscriptions",
            json!({"peer":peer,"offset":""}),
            &format!("extra/payments/{key}/subscriptions"),
        )
        .await
    }

    async fn extra_gifts(&self, key: &str, peer: &Value) -> Result<()> {
        self.extra_collect(
            "payments.getStarGiftCollections",
            json!({"peer":peer,"hash":"0"}),
            &format!("extra/gifts/{key}/collections"),
            None,
        )
        .await?;
        self.extra_pages(
            "payments.getSavedStarGifts",
            json!({"peer":peer,"offset":"","limit":100}),
            &format!("extra/gifts/{key}/saved"),
        )
        .await
    }

    pub(super) fn prepare_extra_peers(
        &self,
        peers: &[(String, Value, Value, Value)],
    ) -> Result<()> {
        for (key, _, metadata, _) in peers {
            let selected = peer_selected(&self.archive.lock().unwrap().config, metadata)?;
            for category in ["scheduled", "locations", "statistics", "boosts"] {
                self.extra_coverage(&format!("extra/{category}/{key}"), if selected {"not_started"} else {"excluded"}, json!({"reason":if selected {"awaiting collector"} else {"peer excluded by selectors"}}))?;
            }
        }
        Ok(())
    }

    pub(super) async fn extra_peer_collectors(
        &self,
        key: &str,
        input: &Value,
        raw: &Value,
    ) -> Result<()> {
        self.extra_collect(
            "messages.getScheduledHistory",
            json!({"peer":input,"hash":"0"}),
            &format!("extra/scheduled/{key}"),
            None,
        )
        .await?;
        if self
            .extra_collect(
                "messages.getRecentLocations",
                json!({"peer":input,"limit":100,"hash":"0"}),
                &format!("extra/locations/{key}"),
                None,
            )
            .await?
            .is_some()
        {
            self.extra_coverage(&format!("extra/locations/{key}"), "limited", json!({"reason":"Recent locations plus observed message history; not a continuous GPS history","limit":100}))?;
        }
        if !key.starts_with("channel:") {
            for category in ["statistics", "boosts"] {
                self.extra_coverage(
                    &format!("extra/{category}/{key}"),
                    "not_applicable",
                    json!({"reason":"not a channel or supergroup"}),
                )?;
            }
            return Ok(());
        }
        self.extra_collect(
            "premium.getBoostsStatus",
            json!({"peer":input}),
            &format!("extra/boosts/{key}"),
            None,
        )
        .await?;
        if raw["creator"] == true || raw.get("admin_rights").is_some() {
            for gifts in [false, true] {
                self.extra_pages(
                    "premium.getBoostsList",
                    json!({"peer":input,"gifts":gifts,"offset":"","limit":100}),
                    &format!("extra/boosts/{key}/list/{gifts}"),
                )
                .await?;
            }
            self.extra_gifts(key, input).await?;
        }
        // Full info is already captured by peer_collectors; use its current record.
        let full = self
            .archive
            .lock()
            .unwrap()
            .query(&Query {
                key: Some(format!("messages.ChatFull:{key}/profile")),
                ..Default::default()
            })?
            .records
            .into_iter()
            .next();
        let Some(full) = full else {
            self.extra_coverage(
                &format!("extra/statistics/{key}"),
                "incomplete",
                json!({"reason":"full channel info unavailable"}),
            )?;
            return Ok(());
        };
        let info = &full.data["full_chat"];
        if info["can_view_stars_revenue"] == true {
            self.extra_finances(key, input).await?;
            self.extra_collect(
                "payments.getStarsRevenueStats",
                json!({"peer":input}),
                &format!("extra/payments/{key}/revenue"),
                None,
            )
            .await?;
        }
        if info["can_view_stats"] != true {
            self.extra_coverage(
                &format!("extra/statistics/{key}"),
                "inaccessible",
                json!({"reason":"can_view_stats is absent"}),
            )?;
            return Ok(());
        }
        let Some(dc) = integer(&info["stats_dc"]) else {
            self.extra_coverage(
                &format!("extra/statistics/{key}"),
                "incomplete",
                json!({"reason":"stats_dc missing"}),
            )?;
            return Ok(());
        };
        let method = if raw["broadcast"] == true {
            "stats.getBroadcastStats"
        } else {
            "stats.getMegagroupStats"
        };
        let scope = format!("extra/statistics/{key}");
        let channel =
            json!({"_":"inputChannel","channel_id":raw["id"],"access_hash":raw["access_hash"]});
        if let Some(stats) = self
            .extra_collect(method, json!({"channel":channel}), &scope, Some(dc as i32))
            .await?
        {
            let mut failed = false;
            for (field, graph) in stats.as_object().into_iter().flatten() {
                if graph["_"] == "statsGraphError" {
                    failed = true;
                }
                if graph["_"] != "statsGraphAsync" {
                    continue;
                }
                let graph = self
                    .extra_collect(
                        "stats.loadAsyncGraph",
                        json!({"token":graph["token"]}),
                        &format!("{scope}/{field}"),
                        Some(dc as i32),
                    )
                    .await?;
                if graph.as_ref().is_none_or(|g| g["_"] != "statsGraph") {
                    failed = true;
                    self.extra_coverage(
                        &format!("{scope}/{field}"),
                        "incomplete",
                        json!({"reason":"graph failed or is still asynchronous"}),
                    )?;
                }
            }
            self.extra_coverage(
                &scope,
                if failed { "incomplete" } else { "complete" },
                json!({"method":method,"dc":dc,"period":stats["period"]}),
            )?;
        }
        Ok(())
    }

    /// Scan retained objects, so a crash between a list response and enrichment cannot lose work.
    pub(super) async fn extra_enrichment(&self) -> Result<()> {
        for kind in ["quick_reply", "gift", "story", "payment", "bot_app"] {
            let mut query = Query {
                kind: Some(kind.into()),
                ..Default::default()
            };
            let mut seen = HashSet::new();
            loop {
                self.check_stop()?;
                let page = self.archive.lock().unwrap().query(&query)?;
                for record in page.records {
                    if record.deleted {
                        continue;
                    }
                    let value = &record.data;
                    match kind {
                        "quick_reply" => {
                            let Some(id) = integer(&value["shortcut_id"]) else {
                                continue;
                            };
                            self.extra_collect(
                                "messages.getQuickReplyMessages",
                                json!({"shortcut_id":id,"hash":"0"}),
                                &format!("extra/business/quick_reply:{id}"),
                                None,
                            )
                            .await?;
                        }
                        "gift" => {
                            let Some(slug) = value["slug"].as_str() else {
                                continue;
                            };
                            if seen.insert(slug.to_string()) {
                                self.extra_collect(
                                    "payments.getUniqueStarGift",
                                    json!({"slug":slug}),
                                    &format!("extra/gifts/unique/{slug}"),
                                    None,
                                )
                                .await?;
                            }
                        }
                        "payment" if value["_"] == "messageActionPaymentSent" => {
                            let Some(peer) = record.metadata["peer"].as_str() else {
                                continue;
                            };
                            let Some(id) = integer(&record.metadata["message_id"]) else {
                                continue;
                            };
                            if !self.extra_peer_selected(peer)? {
                                continue;
                            }
                            let scope = format!("extra/payments/{peer}/receipt:{id}");
                            let checkpoint = format!("receipt:{}:{peer}:{id}", self.job);
                            if self
                                .archive
                                .lock()
                                .unwrap()
                                .checkpoint(&checkpoint)?
                                .is_some()
                            {
                                continue;
                            }
                            let input = self.input_peer(peer)?;
                            if self
                                .extra_collect(
                                    "payments.getPaymentReceipt",
                                    json!({"peer":input,"msg_id":id}),
                                    &scope,
                                    None,
                                )
                                .await?
                                .is_some()
                            {
                                self.archive
                                    .lock()
                                    .unwrap()
                                    .set_checkpoint(&checkpoint, &json!(true))?;
                            }
                        }
                        "story" => {
                            let Some(peer) = record.metadata["story_peer"].as_str() else {
                                continue;
                            };
                            let Some(id) = integer(&value["id"]) else {
                                continue;
                            };
                            if !seen.insert(format!("{peer}:{id}"))
                                || !self.extra_peer_selected(peer)?
                            {
                                continue;
                            }
                            let input = self.input_peer(peer)?;
                            let self_id = self
                                .archive
                                .lock()
                                .unwrap()
                                .checkpoint("account_id")?
                                .and_then(|v| integer(&v))
                                .unwrap_or(0);
                            let scope = format!("extra/story_viewers/{peer}/story:{id}");
                            if peer == format!("user:{self_id}") {
                                self.extra_collect(
                                    "stories.getStoriesViews",
                                    json!({"peer":input,"id":[id]}),
                                    &format!("{scope}/counts"),
                                    None,
                                )
                                .await?;
                                if value["views"]["has_viewers"] == true {
                                    self.extra_pages(
                                        "stories.getStoryViewsList",
                                        json!({"peer":input,"id":id,"offset":"","limit":100}),
                                        &scope,
                                    )
                                    .await?;
                                } else {
                                    self.extra_coverage(&scope, "limited", json!({"reason":"viewer list unavailable or expired","views":value["views"]}))?;
                                }
                            } else if peer.starts_with("channel:") {
                                let raw: Value = serde_json::from_str(
                                    &self.archive.lock().unwrap().db.query_row(
                                        "SELECT raw FROM peers WHERE key=?1",
                                        [peer],
                                        |r| r.get::<_, String>(0),
                                    )?,
                                )?;
                                if raw["creator"] == true || raw.get("admin_rights").is_some() {
                                    self.extra_pages(
                                        "stories.getStoryReactionsList",
                                        json!({"peer":input,"id":id,"offset":"","limit":100}),
                                        &format!("{scope}/reactions"),
                                    )
                                    .await?;
                                }
                                self.extra_coverage(&scope, "limited", json!({"reason":"Channel story API exposes reactions, not viewer identities"}))?;
                            }
                        }
                        "bot_app" => {
                            if let (Some(id), Some(hash)) =
                                (value.get("id"), value.get("access_hash"))
                            {
                                let scope = format!("extra/bots/app:{id}");
                                self.extra_collect("messages.getBotApp", json!({"app":{"_":"inputBotAppID","id":id,"access_hash":hash},"hash":"0"}), &scope, None).await?;
                            }
                        }
                        _ => {}
                    }
                }
                let Some(cursor) = page.next_cursor else {
                    break;
                };
                query.cursor = Some(cursor);
            }
        }
        Ok(())
    }

    pub(super) async fn refresh_extra_media(&self, ctx: &Value) -> Result<bool> {
        if let Some(slug) = ctx["gift_slug"].as_str() {
            return Ok(self
                .extra_collect(
                    "payments.getUniqueStarGift",
                    json!({"slug":slug}),
                    &format!("extra/gifts/unique/{slug}"),
                    None,
                )
                .await?
                .is_some());
        }
        if let Some(reference) = ctx.get("saved_gift_reference") {
            let scope = ctx["scope"].as_str().unwrap_or("extra/gifts/refresh");
            return Ok(self
                .extra_collect(
                    "payments.getSavedStarGift",
                    json!({"stargift":[reference]}),
                    scope,
                    None,
                )
                .await?
                .is_some());
        }
        if let (Some(peer), Some(id)) = (ctx["story_peer"].as_str(), integer(&ctx["story_id"])) {
            let input = self.input_peer(peer)?;
            return Ok(self
                .extra_collect(
                    "stories.getStoriesByID",
                    json!({"peer":input,"id":[id]}),
                    &format!("{peer}/story:{id}/refresh"),
                    None,
                )
                .await?
                .is_some());
        }
        Ok(false)
    }

    fn extra_peer_selected(&self, peer: &str) -> Result<bool> {
        let a = self.archive.lock().unwrap();
        let Some(metadata) = peer_metadata(&a, peer)? else {
            return Ok(false);
        };
        peer_selected(&a.config, &metadata)
    }
}

/// Stable object identities never depend on the page on which an object was encountered.
pub(super) fn object_identity(root: &str, v: &Value, scope: &str) -> Option<(String, String)> {
    let scope = base_scope(scope);
    let kind = match root {
        "StarsTransaction" => "stars_transaction",
        "StarsSubscription" => "stars_subscription",
        "StarGift" => "gift",
        "SavedStarGift" => "saved_gift",
        "StarGiftCollection" => "gift_collection",
        "Boost" | "MyBoost" => "boost",
        "QuickReply" => "quick_reply",
        "StoryView" => "story_viewer",
        "StoryReaction" => "story_reaction",
        "BotApp" | "AttachMenuBot" | "BotInfo" | "BotAppSettings" => "bot_app",
        "ConnectedBot"
        | "BusinessChatLink"
        | "BusinessWorkHours"
        | "BusinessLocation"
        | "BusinessGreetingMessage"
        | "BusinessAwayMessage"
        | "BusinessIntro" => "business",
        "payments.PaymentReceipt" => "payment_receipt",
        "StatsGraph" => "statistics",
        _ => return None,
    };
    let fields: &[&str] = if root == "SavedStarGift" {
        &["saved_id", "msg_id"]
    } else {
        &["id", "shortcut_id", "slot", "bot_id", "user_id", "link"]
    };
    let id = fields
        .iter()
        .find_map(|key| {
            v.get(*key).filter(|v| !v.is_null()).map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| v.to_string())
            })
        })
        .or_else(|| {
            peer_key(&v["message"]["peer_id"])
                .map(|p| format!("{p}/message:{}", v["message"]["id"]))
        })
        .or_else(|| {
            peer_key(&v["peer_id"]).map(|p| {
                if v["story"]["id"].is_null() {
                    p
                } else {
                    format!("{p}/story:{}", v["story"]["id"])
                }
            })
        })
        .unwrap_or_else(|| {
            if root == "SavedStarGift" {
                blake3::hash(v.to_string().as_bytes()).to_hex().to_string()
            } else {
                root.to_string()
            }
        });
    // Catalog gifts are global; saved instances and transactions are scoped to their owner.
    let owner = if root == "StarGift" {
        "telegram"
    } else if root == "QuickReply" {
        "account"
    } else {
        scope
    };
    Some((format!("{owner}/{kind}:{root}:{id}"), kind.into()))
}

pub(super) fn envelope_kind(source: &str) -> &'static str {
    match source {
        "payments.getStarsStatus" => "stars_balance",
        "payments.getStarsRevenueStats" => "stars_revenue",
        "payments.getSavedInfo" => "payment_info",
        "stats.getBroadcastStats" | "stats.getMegagroupStats" | "stats.loadAsyncGraph" => {
            "statistics"
        }
        "premium.getBoostsStatus" => "boost_status",
        "account.getWebAuthorizations" | "contacts.getTopPeers" => "bot_activity",
        "stories.getStoriesViews" => "story_views",
        _ => "rpc",
    }
}

pub(super) fn message_scope(v: &Value, source: &str, scope: &str, scheduled: bool) -> String {
    if let Some(shortcut) = integer(&v["quick_reply_shortcut_id"])
        .map(|v| v.to_string())
        .or_else(|| {
            scope
                .split("quick_reply:")
                .nth(1)
                .map(|v| v.split('/').next().unwrap_or(v).to_string())
        })
    {
        return format!("account/quick_reply:{shortcut}");
    }
    let peer = peer_key(&v["peer_id"]).unwrap_or_else(|| base_scope(scope).into());
    if scheduled
        || matches!(
            source,
            "messages.getScheduledHistory" | "messages.getScheduledMessages"
        )
    {
        format!("{peer}/scheduled")
    } else {
        peer
    }
}

pub(super) fn message_identity(v: &Value, scope: &str) -> Option<(String, String)> {
    let id = integer(&v["id"])?;
    if scope.ends_with("/scheduled") {
        Some((
            format!(
                "{}/scheduled_message:{id}",
                scope.trim_end_matches("/scheduled")
            ),
            "scheduled_message".into(),
        ))
    } else if scope.starts_with("account/quick_reply:") {
        Some((
            format!("{scope}/message:{id}"),
            "quick_reply_message".into(),
        ))
    } else {
        None
    }
}

pub(super) fn message_child_identity(
    root: &str,
    v: &Value,
    parent_key: &str,
) -> Option<(String, String)> {
    let name = v["_"].as_str()?;
    let kind = match root {
        "MessageAction" if name.contains("PhoneCall") || name.contains("GroupCall") => "call",
        "MessageAction"
            if name.contains("Payment")
                || name.contains("GiftPremium")
                || name.contains("GiftStars") =>
        {
            "payment"
        }
        "MessageAction" if name.contains("StarGift") => "gift_event",
        "MessageAction" if name.contains("Boost") => "boost_event",
        "MessageAction" if name.contains("BotAllowed") || name.contains("WebView") => {
            "bot_activity"
        }
        "MessageMedia"
            if matches!(
                name,
                "messageMediaGeo" | "messageMediaGeoLive" | "messageMediaVenue"
            ) =>
        {
            "location"
        }
        _ => return None,
    };
    Some((format!("{parent_key}/{kind}"), kind.into()))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{fixture, message};
    use super::*;
    use crate::config::Config;

    fn mock(e: &mut Engine, replies: Vec<MockReply>) {
        e.mock_rpc = Some(Mutex::new(replies.into()));
    }
    fn subscriptions(next: Option<&str>) -> Value {
        let mut v = json!({"_":"payments.starsStatus","balance":{"_":"starsAmount","amount":"5","nanos":0},"subscriptions":[],"chats":[],"users":[]});
        if let Some(next) = next {
            v["subscriptions_next_offset"] = json!(next);
        }
        v
    }
    fn subscription_reply(offset: &str, result: std::result::Result<Value, String>) -> MockReply {
        MockReply {
            method: "payments.getStarsSubscriptions",
            args: json!({"peer":{"_":"inputPeerSelf"},"offset":offset}),
            dc: None,
            result,
        }
    }
    fn status(e: &Engine, scope: &str) -> String {
        e.archive
            .lock()
            .unwrap()
            .db
            .query_row("SELECT status FROM coverage WHERE name=?1", [scope], |r| {
                r.get(0)
            })
            .unwrap()
    }

    #[tokio::test]
    async fn opaque_pages_resume_empty_continuation_and_preserve_envelopes() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(dir.path(), Config::default()).await;
        let scope = "extra/payments/self/subscriptions";
        mock(
            &mut e,
            vec![
                subscription_reply("", Ok(subscriptions(Some("second")))),
                subscription_reply("second", Err("FLOOD_WAIT_1".into())),
            ],
        );
        let args = json!({"peer":{"_":"inputPeerSelf"},"offset":""});
        e.extra_pages("payments.getStarsSubscriptions", args.clone(), scope)
            .await
            .unwrap();
        assert_eq!(status(&e, scope), "incomplete");
        let cp = e
            .archive
            .lock()
            .unwrap()
            .checkpoint(&format!("extra:{}:{scope}", e.job))
            .unwrap()
            .unwrap();
        assert_eq!(cp["args"]["offset"], "second");
        assert_eq!(cp["complete"], false);
        mock(
            &mut e,
            vec![subscription_reply("second", Ok(subscriptions(None)))],
        );
        e.extra_pages("payments.getStarsSubscriptions", args, scope)
            .await
            .unwrap();
        assert_eq!(status(&e, scope), "complete");
        let a = e.archive.lock().unwrap();
        let records = a
            .query(&Query {
                kind: Some("rpc".into()),
                ..Default::default()
            })
            .unwrap()
            .records;
        assert_eq!(
            records.len(),
            2,
            "both pages survive the default current-object export"
        );
        a.verify().unwrap();
    }

    #[tokio::test]
    async fn repeated_cursors_are_incomplete_and_cancellation_propagates() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(dir.path(), Config::default()).await;
        let scope = "extra/payments/self/subscriptions";
        mock(
            &mut e,
            vec![
                subscription_reply("", Ok(subscriptions(Some("x")))),
                subscription_reply("x", Ok(subscriptions(Some("y")))),
                subscription_reply("y", Ok(subscriptions(Some("x")))),
            ],
        );
        let args = json!({"peer":{"_":"inputPeerSelf"},"offset":""});
        e.extra_pages("payments.getStarsSubscriptions", args.clone(), scope)
            .await
            .unwrap();
        assert_eq!(status(&e, scope), "incomplete");
        e.stop.cancel();
        assert!(
            e.extra_pages("payments.getStarsSubscriptions", args, scope)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn message_namespaces_updates_deletions_and_attachment_context() {
        let dir = tempfile::tempdir().unwrap();
        let e = fixture(dir.path(), Config::default()).await;
        let mut ordinary = message();
        ordinary["date"] = json!(2_100_000_000); // Future scheduled date must not outrank its deletion.
        let bytes = e.schema.encode("Message", &ordinary).unwrap();
        e.capture("Message", &bytes, "history", Some("user:42"), None, None)
            .unwrap();
        let update = json!({"_":"updateNewScheduledMessage","message":ordinary});
        let bytes = e.schema.encode("Update", &update).unwrap();
        e.capture("Update", &bytes, "live", None, None, None)
            .unwrap();
        let mut quick = ordinary.clone();
        quick["quick_reply_shortcut_id"] = json!(7);
        let bytes = e
            .schema
            .encode(
                "Update",
                &json!({"_":"updateQuickReplyMessage","message":quick}),
            )
            .unwrap();
        e.capture("Update", &bytes, "live", None, None, None)
            .unwrap();
        let deletion = json!({"_":"updates","updates":[{"_":"updateDeleteScheduledMessages","peer":{"_":"peerUser","user_id":"42"},"messages":[17]},{"_":"updateDeleteQuickReplyMessages","shortcut_id":7,"messages":[17]}],"users":[],"chats":[],"date":1,"seq":1});
        let bytes = e.schema.encode("Updates", &deletion).unwrap();
        e.capture("Updates", &bytes, "live", None, None, None)
            .unwrap();
        let a = e.archive.lock().unwrap();
        for (kind, key, deleted) in [
            ("message", "user:42/message:17", false),
            ("scheduled_message", "user:42/scheduled_message:17", true),
            (
                "quick_reply_message",
                "account/quick_reply:7/message:17",
                true,
            ),
        ] {
            let records = a
                .query(&Query {
                    kind: Some(kind.into()),
                    ..Default::default()
                })
                .unwrap()
                .records;
            assert_eq!(records.len(), 1, "{kind}");
            assert_eq!(records[0].key, key);
            assert_eq!(records[0].deleted, deleted);
        }
        let contexts: Vec<String> =
            a.db.prepare("SELECT metadata FROM observations WHERE kind='media'")
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
        assert!(contexts.iter().any(|s| s.contains("quick_reply_message") && s.contains("quick_reply_shortcut_id")));
        assert!(contexts.iter().any(|s| s.contains("scheduled_message")));
        a.verify().unwrap();
    }

    #[tokio::test]
    async fn native_projections_and_human_exports_preserve_details() {
        let dir = tempfile::tempdir().unwrap();
        let e = fixture(dir.path(), Config::default()).await;
        for (id, action) in [
            (
                1,
                json!({"_":"messageActionPhoneCall","call_id":"99","duration":42}),
            ),
            (
                2,
                json!({"_":"messageActionPaymentSent","currency":"XTR","total_amount":"17"}),
            ),
        ] {
            let value = json!({"_":"messageService","id":id,"peer_id":{"_":"peerUser","user_id":"42"},"date":1,"action":action});
            let bytes = e.schema.encode("Message", &value).unwrap();
            e.capture("Message", &bytes, "history", Some("user:42"), None, None)
                .unwrap();
        }
        let a = e.archive.lock().unwrap();
        for (kind, expected) in [("call", "duration"), ("payment", "total_amount")] {
            let query = Query {
                kind: Some(kind.into()),
                ..Default::default()
            };
            let records = a.query(&query).unwrap().records;
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].metadata["peer"], "user:42");
            for format in [
                crate::export::Format::Json,
                crate::export::Format::Ndjson,
                crate::export::Format::Txt,
                crate::export::Format::Html,
            ] {
                let mut out = Vec::new();
                crate::export::export(&a, &query, format, &mut out, None).unwrap();
                assert!(String::from_utf8(out).unwrap().contains(expected));
            }
        }
    }

    #[test]
    fn string_identifiers_and_page_independent_identity() {
        let v = json!({"_":"starsTransaction","id":"payment/abc"});
        assert_eq!(
            object_identity(
                "StarsTransaction",
                &v,
                "extra/payments/self/transactions/page:a"
            ),
            object_identity(
                "StarsTransaction",
                &v,
                "extra/payments/self/transactions/page:b"
            )
        );
        assert!(
            object_identity("StarsTransaction", &v, "extra/payments/self/transactions")
                .unwrap()
                .0
                .contains("payment/abc")
        );
        let empty = json!({"_":"payments.starsStatus","subscriptions":[],"subscriptions_next_offset":"more"});
        assert_eq!(
            next_page(
                "payments.getStarsSubscriptions",
                &json!({"offset":""}),
                &empty
            )
            .unwrap()
            .unwrap()["offset"],
            "more"
        );
    }
    #[tokio::test]
    async fn scheduled_filters_slice_permissions_and_duration_have_honest_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(
            dir.path(),
            Config {
                history_selector: "outgoing = true".into(),
                attachment_selector: "false".into(),
                ..Default::default()
            },
        )
        .await;
        let args =
            json!({"peer":{"_":"inputPeerUser","user_id":"42","access_hash":"123"},"hash":"0"});
        let scope = "extra/scheduled/user:42";
        for (response, expected) in [
            (
                Ok(
                    json!({"_":"messages.messages","messages":[message()],"topics":[],"users":[],"chats":[]}),
                ),
                "limited",
            ),
            (
                Ok(
                    json!({"_":"messages.messagesSlice","count":20,"messages":[],"topics":[],"users":[],"chats":[]}),
                ),
                "incomplete",
            ),
            (Err("CHAT_ADMIN_REQUIRED".into()), "inaccessible"),
            (
                Ok(
                    json!({"_":"messages.messages","messages":[],"topics":[],"users":[],"chats":[]}),
                ),
                "complete",
            ),
        ] {
            mock(
                &mut e,
                vec![MockReply {
                    method: "messages.getScheduledHistory",
                    args: args.clone(),
                    dc: None,
                    result: response,
                }],
            );
            e.extra_collect("messages.getScheduledHistory", args.clone(), scope, None)
                .await
                .unwrap();
            assert_eq!(status(&e, scope), expected);
        }
        assert!(
            e.archive
                .lock()
                .unwrap()
                .query(&Query {
                    kind: Some("scheduled_message".into()),
                    ..Default::default()
                })
                .unwrap()
                .records
                .is_empty()
        );
        e.options.max_seconds = Some(0);
        assert!(
            e.extra_collect("messages.getScheduledHistory", args, scope, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn statistics_use_stats_dc_and_graph_errors_mark_collection_incomplete() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(dir.path(), Config::default()).await;
        let raw = json!({"_":"channel","id":"123","access_hash":"456","title":"Channel","broadcast":true,"photo":{"_":"chatPhotoEmpty"},"date":1});
        let input = json!({"_":"inputPeerChannel","channel_id":"123","access_hash":"456"});
        let full = json!({"_":"messages.chatFull","full_chat":{"_":"channelFull","id":"123","about":"","can_view_stats":true,"stats_dc":4,"read_inbox_max_id":0,"read_outbox_max_id":0,"unread_count":0,"chat_photo":{"_":"photoEmpty","id":"0"},"notify_settings":{"_":"peerNotifySettings"},"bot_info":[],"pts":1},"chats":[raw],"users":[]});
        let bytes = e.schema.encode("messages.ChatFull", &full).unwrap();
        e.capture(
            "messages.ChatFull",
            &bytes,
            "channels.getFullChannel",
            Some("channel:123/profile"),
            None,
            None,
        )
        .unwrap();
        let mut stats = json!({"_":"stats.broadcastStats","period":{"_":"statsDateRangeDays","min_date":1,"max_date":2},"enabled_notifications":{"_":"statsPercentValue","part":1.0,"total":2.0},"recent_posts_interactions":[]});
        for field in [
            "followers",
            "views_per_post",
            "shares_per_post",
            "reactions_per_post",
            "views_per_story",
            "shares_per_story",
            "reactions_per_story",
        ] {
            stats[field] = json!({"_":"statsAbsValueAndPrev","current":1.0,"previous":0.0});
        }
        for field in [
            "growth_graph",
            "followers_graph",
            "mute_graph",
            "top_hours_graph",
            "interactions_graph",
            "iv_interactions_graph",
            "views_by_source_graph",
            "new_followers_by_source_graph",
            "languages_graph",
            "reactions_by_emotion_graph",
            "story_interactions_graph",
            "story_reactions_by_emotion_graph",
        ] {
            stats[field] = json!({"_":"statsGraph","json":{"_":"dataJSON","data":"{}"}});
        }
        stats["growth_graph"] = json!({"_":"statsGraphAsync","token":"pending"});
        mock(
            &mut e,
            vec![
                MockReply {
                    method: "messages.getScheduledHistory",
                    args: json!({"peer":input,"hash":"0"}),
                    dc: None,
                    result: Ok(
                        json!({"_":"messages.messages","messages":[],"topics":[],"chats":[],"users":[]}),
                    ),
                },
                MockReply {
                    method: "messages.getRecentLocations",
                    args: json!({"peer":input,"limit":100,"hash":"0"}),
                    dc: None,
                    result: Ok(
                        json!({"_":"messages.messages","messages":[],"topics":[],"chats":[],"users":[]}),
                    ),
                },
                MockReply {
                    method: "premium.getBoostsStatus",
                    args: json!({"peer":input}),
                    dc: None,
                    result: Ok(
                        json!({"_":"premium.boostsStatus","level":0,"current_level_boosts":0,"boosts":0,"boost_url":"https://t.me/boost/example"}),
                    ),
                },
                MockReply {
                    method: "stats.getBroadcastStats",
                    args: json!({"channel":{"_":"inputChannel","channel_id":"123","access_hash":"456"}}),
                    dc: Some(4),
                    result: Ok(stats),
                },
                MockReply {
                    method: "stats.loadAsyncGraph",
                    args: json!({"token":"pending"}),
                    dc: Some(4),
                    result: Ok(json!({"_":"statsGraphError","error":"not ready"})),
                },
            ],
        );
        e.extra_peer_collectors("channel:123", &input, &raw)
            .await
            .unwrap();
        assert_eq!(status(&e, "extra/statistics/channel:123"), "incomplete");
        assert_eq!(
            status(&e, "extra/statistics/channel:123/growth_graph"),
            "incomplete"
        );
        assert!(e.mock_rpc.as_ref().unwrap().lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn story_owners_and_expired_viewers_are_not_confused() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(dir.path(), Config::default()).await;
        cache_peer(
            &e.archive.lock().unwrap(),
            &json!({"_":"user","self":true,"id":"1","first_name":"Self"}),
            1,
        )
        .unwrap();
        let story = json!({"_":"storyItem","id":3,"date":1,"expire_date":2,"media":{"_":"messageMediaEmpty"},"views":{"_":"storyViews","views_count":1}});
        let value = json!({"_":"stories.allStories","count":2,"state":"x","peer_stories":[{"_":"peerStories","peer":{"_":"peerUser","user_id":"1"},"stories":[story]},{"_":"peerStories","peer":{"_":"peerUser","user_id":"42"},"stories":[story]}],"chats":[],"users":[],"stealth_mode":{"_":"storiesStealthMode"}});
        let bytes = e.schema.encode("stories.AllStories", &value).unwrap();
        e.capture(
            "stories.AllStories",
            &bytes,
            "stories.getAllStories",
            Some("stories"),
            None,
            None,
        )
        .unwrap();
        let records = e
            .archive
            .lock()
            .unwrap()
            .query(&Query {
                kind: Some("story".into()),
                ..Default::default()
            })
            .unwrap()
            .records;
        assert_eq!(records.len(), 2);
        assert_ne!(records[0].key, records[1].key);
        mock(
            &mut e,
            vec![MockReply {
                method: "stories.getStoriesViews",
                args: json!({"peer":{"_":"inputPeerSelf"},"id":[3]}),
                dc: None,
                result: Ok(
                    json!({"_":"stories.storyViews","views":[{"_":"storyViews","views_count":1}],"users":[]}),
                ),
            }],
        );
        e.extra_enrichment().await.unwrap();
        assert_eq!(status(&e, "extra/story_viewers/user:1/story:3"), "limited");
        assert!(e.mock_rpc.as_ref().unwrap().lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn separate_transaction_ids_survive_shared_message_and_remote_export() {
        use axum::{
            body::{Body, to_bytes},
            http::Request,
        };
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let e = fixture(dir.path(), Config::default()).await;
        let transaction = |id: &str| json!({"_":"starsTransaction","id":id,"msg_id":17,"amount":{"_":"starsAmount","amount":"5","nanos":0},"date":1,"peer":{"_":"starsTransactionPeerAppStore"}});
        let value = json!({"_":"payments.starsStatus","balance":{"_":"starsAmount","amount":"10","nanos":0},"history":[transaction("one"),transaction("two")],"chats":[],"users":[]});
        let bytes = e.schema.encode("payments.StarsStatus", &value).unwrap();
        e.capture(
            "payments.StarsStatus",
            &bytes,
            "payments.getStarsTransactions",
            Some("extra/payments/self/transactions/page:1"),
            None,
            None,
        )
        .unwrap();
        let query = Query {
            kind: Some("stars_transaction".into()),
            all_versions: true,
            ..Default::default()
        };
        let mut local = Vec::new();
        crate::export::export(
            &e.archive.lock().unwrap(),
            &query,
            crate::export::Format::Json,
            &mut local,
            None,
        )
        .unwrap();
        let router = crate::http::router(crate::http::ApiState {
            root: dir.path().into(),
            token: None,
        });
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/query")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&query).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
                .unwrap();
        assert_eq!(body["records"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["records"],
            serde_json::from_slice::<Value>(&local).unwrap()
        );
        drop(e);
        let reopened = Archive::open(dir.path(), false).unwrap();
        assert_eq!(
            reopened
                .query(&Query {
                    kind: Some("stars_transaction".into()),
                    ..Default::default()
                })
                .unwrap()
                .records
                .len(),
            2
        );
        reopened.verify().unwrap();
    }
    #[tokio::test]
    async fn account_endpoints_encode_on_pinned_layer_and_archive_empty_results() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(dir.path(), Config::default()).await;
        let peer = json!({"_":"inputPeerSelf"});
        let empty_stars = subscriptions(None);
        let replies = vec![
            (
                "payments.getSavedInfo",
                json!({}),
                json!({"_":"payments.savedInfo"}),
            ),
            (
                "premium.getMyBoosts",
                json!({}),
                json!({"_":"premium.myBoosts","my_boosts":[],"chats":[],"users":[]}),
            ),
            (
                "account.getConnectedBots",
                json!({}),
                json!({"_":"account.connectedBots","connected_bots":[],"users":[]}),
            ),
            (
                "account.getBusinessChatLinks",
                json!({}),
                json!({"_":"account.businessChatLinks","links":[],"chats":[],"users":[]}),
            ),
            (
                "messages.getAttachMenuBots",
                json!({"hash":"0"}),
                json!({"_":"attachMenuBots","hash":"0","bots":[],"users":[]}),
            ),
            (
                "messages.searchGlobal",
                json!({"q":"","filter":{"_":"inputMessagesFilterPhoneCalls"},"min_date":0,"max_date":0,"offset_rate":0,"offset_peer":{"_":"inputPeerEmpty"},"offset_id":0,"limit":100}),
                json!({"_":"messages.messages","messages":[],"topics":[],"chats":[],"users":[]}),
            ),
            (
                "payments.getStarsStatus",
                json!({"peer":peer}),
                empty_stars.clone(),
            ),
            (
                "payments.getStarsTransactions",
                json!({"peer":peer,"offset":"","limit":100}),
                empty_stars.clone(),
            ),
            (
                "payments.getStarsSubscriptions",
                json!({"peer":peer,"offset":""}),
                empty_stars,
            ),
            (
                "payments.getStarGiftCollections",
                json!({"peer":peer,"hash":"0"}),
                json!({"_":"payments.starGiftCollections","collections":[]}),
            ),
            (
                "payments.getSavedStarGifts",
                json!({"peer":peer,"offset":"","limit":100}),
                json!({"_":"payments.savedStarGifts","count":0,"gifts":[],"chats":[],"users":[]}),
            ),
        ];
        mock(
            &mut e,
            replies
                .into_iter()
                .map(|(method, args, result)| MockReply {
                    method,
                    args,
                    dc: None,
                    result: Ok(result),
                })
                .collect(),
        );
        e.prepare_extra_coverage().unwrap();
        e.extra_account_collectors().await.unwrap();
        e.finish_extra_coverage().unwrap();
        assert!(e.mock_rpc.as_ref().unwrap().lock().unwrap().is_empty());
        assert_eq!(status(&e, "extra/payments"), "complete");
        assert_eq!(status(&e, "extra/local_data"), "unsupported");
        assert_eq!(
            status(&e, "extra/local_data/saved_contacts"),
            "requires_takeout"
        );
        assert_eq!(status(&e, "extra/boosts"), "limited");
    }

    #[tokio::test]
    async fn gift_and_quick_reply_enrichment_uses_retained_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(dir.path(), Config::default()).await;
        let reply = json!({"_":"messages.quickReplies","quick_replies":[{"_":"quickReply","shortcut_id":7,"shortcut":"hello","top_message":17,"count":2}],"messages":[message()],"chats":[],"users":[]});
        let bytes = e.schema.encode("messages.QuickReplies", &reply).unwrap();
        e.capture(
            "messages.QuickReplies",
            &bytes,
            "messages.getQuickReplies",
            None,
            None,
            None,
        )
        .unwrap();
        let gift = json!({"_":"starGiftUnique","id":"123","gift_id":"42","title":"Gift","slug":"gift-123","num":123,"attributes":[],"availability_issued":1,"availability_total":2});
        let saved = json!({"_":"payments.savedStarGifts","count":1,"gifts":[{"_":"savedStarGift","date":1,"msg_id":8,"gift":gift}],"chats":[],"users":[]});
        let bytes = e.schema.encode("payments.SavedStarGifts", &saved).unwrap();
        e.capture(
            "payments.SavedStarGifts",
            &bytes,
            "payments.getSavedStarGifts",
            Some("extra/gifts/self/saved/page:1"),
            None,
            None,
        )
        .unwrap();
        // Enrichment derives work from the durable archive, without keeping the RPC response in memory.
        let mut second = message();
        second["id"] = json!(18);
        mock(
            &mut e,
            vec![
                MockReply {
                    method: "messages.getQuickReplyMessages",
                    args: json!({"shortcut_id":7,"hash":"0"}),
                    dc: None,
                    result: Ok(
                        json!({"_":"messages.messages","messages":[message(),second],"topics":[],"users":[],"chats":[]}),
                    ),
                },
                MockReply {
                    method: "payments.getUniqueStarGift",
                    args: json!({"slug":"gift-123"}),
                    dc: None,
                    result: Ok(
                        json!({"_":"payments.uniqueStarGift","gift":gift,"chats":[],"users":[]}),
                    ),
                },
            ],
        );
        e.extra_enrichment().await.unwrap();
        let a = e.archive.lock().unwrap();
        assert_eq!(
            a.query(&Query {
                kind: Some("quick_reply_message".into()),
                ..Default::default()
            })
            .unwrap()
            .records
            .len(),
            2
        );
        assert!(
            a.query(&Query {
                kind: Some("message".into()),
                ..Default::default()
            })
            .unwrap()
            .records
            .is_empty()
        );
        assert_eq!(
            a.query(&Query {
                kind: Some("gift".into()),
                ..Default::default()
            })
            .unwrap()
            .records
            .len(),
            1
        );
        assert_eq!(
            a.query(&Query {
                kind: Some("saved_gift".into()),
                ..Default::default()
            })
            .unwrap()
            .records
            .len(),
            1
        );
        drop(a);
        let bytes = e
            .schema
            .encode(
                "Update",
                &json!({"_":"updateDeleteQuickReply","shortcut_id":7}),
            )
            .unwrap();
        e.capture("Update", &bytes, "live", None, None, None)
            .unwrap();
        for kind in ["quick_reply", "quick_reply_message"] {
            assert!(
                e.archive
                    .lock()
                    .unwrap()
                    .query(&Query {
                        kind: Some(kind.into()),
                        ..Default::default()
                    })
                    .unwrap()
                    .records
                    .iter()
                    .all(|record| record.deleted)
            );
        }
    }

    #[tokio::test]
    async fn scheduled_media_refresh_uses_scheduled_api() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(dir.path(), Config::default()).await;
        let response = |m: Value| json!({"_":"messages.messages","messages":[m],"topics":[],"chats":[],"users":[]});
        let bytes = e
            .schema
            .encode("messages.Messages", &response(message()))
            .unwrap();
        e.capture(
            "messages.Messages",
            &bytes,
            "messages.getScheduledHistory",
            Some("extra/scheduled/user:42"),
            None,
            None,
        )
        .unwrap();
        let mut updated = message();
        updated["media"]["photo"]["file_reference"] = json!({"$bytes":"ef00"});
        mock(
            &mut e,
            vec![MockReply {
                method: "messages.getScheduledMessages",
                args: json!({"peer":{"_":"inputPeerUser","user_id":"42","access_hash":"123"},"id":[17]}),
                dc: None,
                result: Ok(response(updated)),
            }],
        );
        e.refresh_media_reference("photo:123:x").await.unwrap();
        let location: String = e
            .archive
            .lock()
            .unwrap()
            .db
            .query_row(
                "SELECT location FROM media WHERE id='photo:123:x'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(location.contains("ef00"));
        assert!(e.mock_rpc.as_ref().unwrap().lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn calls_obey_message_budget_and_resume_with_search_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(dir.path(), Config::default()).await;
        e.options.max_messages = Some(1);
        let args = json!({"q":"","filter":{"_":"inputMessagesFilterPhoneCalls"},"min_date":0,"max_date":0,"offset_rate":0,"offset_peer":{"_":"inputPeerEmpty"},"offset_id":0,"limit":1});
        let call = json!({"_":"messageService","id":17,"peer_id":{"_":"peerUser","user_id":"42"},"date":100,"action":{"_":"messageActionPhoneCall","call_id":"123"}});
        mock(
            &mut e,
            vec![MockReply {
                method: "messages.searchGlobal",
                args: args.clone(),
                dc: None,
                result: Ok(
                    json!({"_":"messages.messagesSlice","count":2,"messages":[call],"topics":[],"chats":[],"users":[]}),
                ),
            }],
        );
        let scope = "extra/calls/history";
        e.extra_pages("messages.searchGlobal", args.clone(), scope)
            .await
            .unwrap();
        assert_eq!(status(&e, scope), "incomplete");
        let cp = e
            .archive
            .lock()
            .unwrap()
            .checkpoint(&format!("extra:{}:{scope}", e.job))
            .unwrap()
            .unwrap();
        assert_eq!(
            cp["args"]["offset_rate"], 100,
            "fallback cursor is the last message date"
        );
        assert_eq!(cp["messages"], 1);
        e.base_messages = 1; // A fresh invocation has a fresh budget.
        mock(
            &mut e,
            vec![MockReply {
                method: "messages.searchGlobal",
                args: cp["args"].clone(),
                dc: None,
                result: Ok(
                    json!({"_":"messages.messages","messages":[],"topics":[],"chats":[],"users":[]}),
                ),
            }],
        );
        e.extra_pages("messages.searchGlobal", args, scope)
            .await
            .unwrap();
        assert_eq!(status(&e, scope), "complete");
    }
    #[tokio::test]
    async fn complete_scheduled_snapshots_remove_stale_entries_but_preserve_newer_updates() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = fixture(dir.path(), Config::default()).await;
        let bytes = e.schema.encode("Message", &message()).unwrap();
        e.capture(
            "Message",
            &bytes,
            "messages.getScheduledHistory",
            Some("extra/scheduled/user:42"),
            None,
            None,
        )
        .unwrap();
        let args =
            json!({"peer":{"_":"inputPeerUser","user_id":"42","access_hash":"123"},"hash":"0"});
        let empty =
            json!({"_":"messages.messages","messages":[],"topics":[],"users":[],"chats":[]});
        mock(
            &mut e,
            vec![MockReply {
                method: "messages.getScheduledHistory",
                args: args.clone(),
                dc: None,
                result: Ok(empty.clone()),
            }],
        );
        e.extra_collect(
            "messages.getScheduledHistory",
            args,
            "extra/scheduled/user:42",
            None,
        )
        .await
        .unwrap();
        let query = Query {
            kind: Some("scheduled_message".into()),
            ..Default::default()
        };
        let record = e
            .archive
            .lock()
            .unwrap()
            .query(&query)
            .unwrap()
            .records
            .remove(0);
        assert!(record.deleted);
        assert_eq!(
            record.metadata["queue_state"],
            "absent_from_complete_snapshot"
        );
        let started = now();
        let update = json!({"_":"updateNewScheduledMessage","message":message()});
        let bytes = e.schema.encode("Update", &update).unwrap();
        e.capture("Update", &bytes, "live", None, None, None)
            .unwrap();
        let snapshot_bytes = e.schema.encode("messages.Messages", &empty).unwrap();
        e.reconcile_scheduled_snapshot(
            "user:42",
            "messages.Messages",
            &snapshot_bytes,
            &empty,
            started,
        )
        .unwrap();
        assert!(!e.archive.lock().unwrap().query(&query).unwrap().records[0].deleted);
    }
}
