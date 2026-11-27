# Release history continuity

Run **Harklight release history verification** on the exact Server candidate.
Its artifact is a Server inventory. Complete the check by running the ordinary
iPhone, iPad and Web flows on a shared disposable fixture and validating their
three result files against that fixture's manifest.

## Fixture

Use extracted, checksum-verified Linux packages for published v0.1.49,
published v0.2.16 and the exact Server candidate. Choose a new private
directory on the isolated lab host and a certificate name the clients can
reach over the approved lab route. The fixture defaults to loopback listeners
and generates a new synthetic owner and CA. Keep private fixture keys and
backup state in the lab.

    scripts/history-continuity/fixture.sh init "$FIXTURE" "$OLD_PACKAGE" "$LAB_NAME"
    scripts/history-continuity/fixture.sh serve "$FIXTURE" "$OLD_PACKAGE"

Enroll the test App devices using the ordinary device flow and the fixture CA.
Use the fixture owner to approve each enrollment. Capture the old App history
before stopping the sole old Server process. Configure the Web sign-in on the
same disposable account using the package's normal dashboard settings.
Keep the same fixture CA, profile and account throughout.

    scripts/history-continuity/fixture.sh upgrade "$FIXTURE" "$CANDIDATE" "$BRIDGE_PACKAGE"
    scripts/history-continuity/fixture.sh serve "$FIXTURE" "$CANDIDATE"

The upgrade retains a private stopped-state backup and checks predecessor
inventory against the upgraded Server. Capture i1 in Inbox, i2 in Replied with
“Synthetic answer”, and i3 with its e1 Chat entry “Synthetic discussion”
through ordinary client navigation after upgrade and relaunch. Use the already
enrolled device profiles. On Web use an attached browser session on the
upgraded fixture.

Each client result JSON records `platform` (iPhone, iPad or Web), exact
`client_build`, `outcome`, observed `observations`, and the matching
`server_version`, `server_commit` and `fixture_sha256` from
`results/manifest.json`. Use UI observations for the six fields listed under
`expected` in the manifest. Preserve the client run evidence alongside its
result, including the ordinary view and history opened on that run.

    scripts/history-continuity/verify.py gate "$FIXTURE/results/manifest.json" \
      iphone.json ipad.json web.json

The continuity check passes when the upgraded Server inventory and all three
ordinary UI results pass on the same fixture and exact Server build.
Captured-response replay is supplemental evidence. The workflow uploads
synthetic inventories only.
