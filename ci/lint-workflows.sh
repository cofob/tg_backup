#!/bin/sh
set -eu
# GitHub added $/ self-repository references in July 2026. Zizmor requires
# them; actionlint 1.7.12 does not recognize them yet. Ignore only that
# obsolete syntax diagnostic for these two existing, separately linted files.
actionlint -ignore '^reusable workflow call "\$/\.github/workflows/(ci|images)\.yml" at "uses" is not following the format'
