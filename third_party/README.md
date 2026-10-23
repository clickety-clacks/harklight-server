# Bundled third-party distribution notices

These are existing runtime/dependency notices, preserved with the server
artifact. They do not select or grant a new license for Harklight server source.

- Erlang/OTP 27.3.4: unchanged LICENSE.txt from the exact upstream source tag:
  https://github.com/erlang/otp/blob/OTP-27.3.4/LICENSE.txt
- Elixir 1.19.5: unchanged LICENSE from the installed release distribution.
- DBConnection 2.10.2: its distributed README includes Copyright 2015 James Fish
  and the Apache-2.0 declaration; include the standard Apache-2.0 license text.
- SQLite: retain the embedded amalgamation's initial authorship/public-domain
  notice as supplied in the pinned Exqlite dependency.

The package builder additionally copies existing LICENSE/NOTICE/COPYING files
from each shipped dependency's source distribution, including Exqlite and
Telemetry. Build-time-only dependencies are not presented as shipped runtimes.
