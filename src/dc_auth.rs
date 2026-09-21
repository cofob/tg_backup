//! Authorization recovery for raw cross-DC requests (outside takeout wrappers).
use crate::tl::RawRequest;
use anyhow::{Context, Result};
use grammers_client::Client;
use grammers_mtsender::InvocationError;
use grammers_tl_types::{Serializable, enums::auth::ExportedAuthorization, functions::auth};
use std::collections::HashSet;

pub(crate) trait Transport {
    async fn invoke(
        &self,
        dc: Option<i32>,
        request: &RawRequest,
    ) -> Result<Vec<u8>, InvocationError>;
}
impl Transport for Client {
    async fn invoke(
        &self,
        dc: Option<i32>,
        request: &RawRequest,
    ) -> Result<Vec<u8>, InvocationError> {
        match dc {
            Some(dc) => self.invoke_in_dc(dc, request).await.map(|r| r.0),
            None => self.invoke(request).await.map(|r| r.0),
        }
    }
}
pub(crate) async fn recover(
    transport: &impl Transport,
    target: i32,
    lock: &tokio::sync::Mutex<()>,
) -> Result<()> {
    use grammers_tl_types::Deserializable;
    let _guard = lock.lock().await;
    let bytes = transport
        .invoke(
            None,
            &RawRequest(auth::ExportAuthorization { dc_id: target }.to_bytes()),
        )
        .await
        .with_context(|| format!("exporting authorization to DC {target}"))?;
    let ExportedAuthorization::Authorization(exported) =
        ExportedAuthorization::from_bytes(&bytes).context("decoding exported authorization")?;
    transport
        .invoke(
            Some(target),
            &RawRequest(
                auth::ImportAuthorization {
                    id: exported.id,
                    bytes: exported.bytes,
                }
                .to_bytes(),
            ),
        )
        .await
        .with_context(|| format!("importing authorization to DC {target}"))?;
    Ok(())
}
pub(crate) fn should_recover(
    error: &InvocationError,
    dc: Option<i32>,
    home: i32,
    attempted: &mut HashSet<i32>,
) -> bool {
    matches!(error, InvocationError::Rpc(e) if e.name == "AUTH_KEY_UNREGISTERED")
        && dc.is_some_and(|dc| dc != home && attempted.insert(dc))
}

/// Keep transport errors typed so the caller can still handle migration/flood waits.
pub(crate) async fn invoke(
    transport: &impl Transport,
    request: &RawRequest,
    dc: Option<i32>,
    home: i32,
    attempted: &mut HashSet<i32>,
    lock: &tokio::sync::Mutex<()>,
) -> Result<Result<crate::tl::RawResponse, InvocationError>> {
    let mut result = transport.invoke(dc, request).await;
    if let Err(ref error) = result
        && should_recover(error, dc, home, attempted)
    {
        recover(transport, dc.unwrap(), lock).await?;
        result = transport.invoke(dc, request).await;
    }
    Ok(result.map(crate::tl::RawResponse))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::VecDeque, sync::Mutex};
    type Reply = (Option<i32>, Vec<u8>, Result<Vec<u8>, InvocationError>);
    struct Fake(Mutex<VecDeque<Reply>>);
    impl Transport for Fake {
        async fn invoke(
            &self,
            dc: Option<i32>,
            request: &RawRequest,
        ) -> Result<Vec<u8>, InvocationError> {
            let (expected_dc, bytes, response) =
                self.0.lock().unwrap().pop_front().expect("unexpected RPC");
            assert_eq!(dc, expected_dc);
            assert_eq!(request.0, bytes);
            response
        }
    }
    fn unauthorized() -> InvocationError {
        InvocationError::Rpc(grammers_mtsender::RpcError::from(
            grammers_tl_types::types::RpcError {
                error_code: 401,
                error_message: "AUTH_KEY_UNREGISTERED".into(),
            },
        ))
    }
    fn export_reply() -> Vec<u8> {
        ExportedAuthorization::Authorization(
            grammers_tl_types::types::auth::ExportedAuthorization {
                id: 42,
                bytes: vec![1, 2, 3],
            },
        )
        .to_bytes()
    }
    fn import_request() -> Vec<u8> {
        auth::ImportAuthorization {
            id: 42,
            bytes: vec![1, 2, 3],
        }
        .to_bytes()
    }
    #[tokio::test]
    async fn transfers_and_retries_exact_takeout_request_once_per_dc() {
        let request = RawRequest(
            grammers_tl_types::functions::InvokeWithTakeout {
                takeout_id: 77,
                query: RawRequest(vec![1, 2, 3, 4]),
            }
            .to_bytes(),
        );
        let mut calls = VecDeque::new();
        for dc in [1, 4] {
            calls.extend([
                (Some(dc), request.0.clone(), Err(unauthorized())),
                (
                    None,
                    auth::ExportAuthorization { dc_id: dc }.to_bytes(),
                    Ok(export_reply()),
                ),
                (Some(dc), import_request(), Ok(vec![])),
                (Some(dc), request.0.clone(), Ok(vec![9])),
            ]);
        }
        calls.push_back((Some(1), request.0.clone(), Err(unauthorized())));
        calls.push_back((Some(2), request.0.clone(), Err(unauthorized())));
        let fake = Fake(Mutex::new(calls));
        let lock = tokio::sync::Mutex::new(());
        let mut attempted = HashSet::new();
        for dc in [1, 4] {
            assert_eq!(
                invoke(&fake, &request, Some(dc), 2, &mut attempted, &lock)
                    .await
                    .unwrap()
                    .unwrap()
                    .0,
                vec![9]
            );
        }
        for dc in [1, 2] {
            assert!(
                invoke(&fake, &request, Some(dc), 2, &mut attempted, &lock)
                    .await
                    .unwrap()
                    .is_err()
            );
        }
        assert!(fake.0.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn failed_export_import_and_repeated_401_are_bounded() {
        for stage in ["export", "import", "repeat"] {
            let request = RawRequest(vec![7]);
            let mut calls = VecDeque::from([(Some(3), vec![7], Err(unauthorized()))]);
            calls.push_back((
                None,
                auth::ExportAuthorization { dc_id: 3 }.to_bytes(),
                if stage == "export" {
                    Err(unauthorized())
                } else {
                    Ok(export_reply())
                },
            ));
            if stage != "export" {
                calls.push_back((
                    Some(3),
                    import_request(),
                    if stage == "import" {
                        Err(unauthorized())
                    } else {
                        Ok(vec![])
                    },
                ));
            }
            if stage == "repeat" {
                calls.push_back((Some(3), vec![7], Err(unauthorized())));
            }
            let fake = Fake(Mutex::new(calls));
            let result = invoke(
                &fake,
                &request,
                Some(3),
                2,
                &mut HashSet::new(),
                &tokio::sync::Mutex::new(()),
            )
            .await;
            if stage == "repeat" {
                assert!(result.unwrap().is_err());
            } else {
                assert!(format!("{:#}", result.err().unwrap()).contains(stage));
            }
            assert!(fake.0.lock().unwrap().is_empty());
        }
    }
}
