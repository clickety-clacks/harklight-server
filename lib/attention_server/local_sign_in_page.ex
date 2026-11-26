defmodule AttentionServer.LocalSignInPage do
  @moduledoc false

  alias AttentionServer.DashboardBrand

  def html do
    """
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <title>Sign in · Harklight</title>
      #{DashboardBrand.favicon()}
      <style>
        :root { color-scheme: light dark; font: 1rem / 1.55 system-ui, sans-serif; }
        body { margin: 0; }
        main { max-width: 25rem; margin: 11vh auto; padding: 1.5rem; }
        h1 { font-size: 1.6rem; line-height: 1.2; }
        label { display: block; }
        input, button { font: inherit; }
        input[name=code] { width: 100%; box-sizing: border-box; padding: .65rem; margin: .35rem 0 1rem; }
        button { padding: .45rem .75rem; cursor: pointer; }
        .preferences { margin-top: 1.25rem; }
        #last-used { font-size: .8rem; margin-inline: .35rem; }
      </style>
    </head>
    <body>
      <main>
        #{DashboardBrand.lockup(decorative: true)}
        <h1>Sign in to Harklight</h1>
        <form id="owner-code" action="/sign-in/code" method="post">
          <label for="code">One-time code <span id="last-used" hidden>Last used</span></label>
          <input id="code" name="code" autocomplete="one-time-code" autocapitalize="characters" maxlength="32" required>
          <button type="submit">Sign in</button>
          <button type="button" id="use-passkey">Use a passkey <span id="passkey-last-used" hidden>Last used</span></button>
          <div class="preferences">
            <label><input type="checkbox" id="remember-method"> Remember my sign-in preference on this device</label>
            <button type="button" id="use-preference">Use my remembered preference</button>
          </div>
        </form>
        <p id="sign-in-status" role="status"></p>
        <noscript>Enable JavaScript to sign in.</noscript>
      </main>
      <script src="/sign-in/code.js" defer></script>
      <script src="/sign-in/passkey.js" defer></script>
    </body>
    </html>
    """
  end

  def script do
    ~S"""
    const form = document.getElementById("owner-code");
    const status = document.getElementById("sign-in-status");
    const remember = document.getElementById("remember-method");
    const preference = document.getElementById("use-preference");
    const marker = document.getElementById("last-used");
    const passkeyMarker = document.getElementById("passkey-last-used");
    const passkey = document.getElementById("use-passkey");
    const storageKey = "harklight.last-sign-in-method";
    const params = new URLSearchParams(location.search);
    const appReturn = params.get("return") === "app";
    const transaction = params.get("transaction");

    if (appReturn) {
      document.querySelector(".preferences").hidden = true;
      passkey.hidden = true;
    }

    preference.addEventListener("click", () => {
      try {
        const method = localStorage.getItem(storageKey);
        marker.hidden = method !== "owner_code";
        passkeyMarker.hidden = method !== "passkey" || passkey.hidden;
      } catch (_) {
        marker.hidden = true;
        passkeyMarker.hidden = true;
      }
    });

    const complete = method => {
      try {
        if (remember.checked && ["owner_code", "passkey"].includes(method)) {
          localStorage.setItem(storageKey, method);
        } else {
          localStorage.removeItem(storageKey);
        }
      } catch (_) {
        // The preference is optional in a profile with storage disabled.
      }
      location.replace("/inbox");
    };

    passkey.addEventListener("harklight-passkey-complete", () => complete("passkey"));

    form.addEventListener("submit", async event => {
      event.preventDefault();
      status.textContent = "";
      if (appReturn && !transaction) {
        status.textContent = "Return to the app and start sign-in again.";
        return;
      }

      const body = {code: document.getElementById("code").value};
      if (appReturn) body.transaction = transaction;

      try {
        const response = await fetch(form.action, {
          method: "POST",
          headers: {"content-type": "application/json"},
          body: JSON.stringify(body)
        });
        if (!response.ok) {
          status.textContent = response.status === 401
            ? "That code was refused. Ask the owner for a fresh code."
            : "Sign-in is unavailable. Try again.";
          return;
        }

        const result = await response.json();
        if (appReturn) {
          if (result.transaction !== transaction) throw new Error("Unexpected transaction");
          location.replace("harklight://sign-in?transaction=" + encodeURIComponent(transaction));
          return;
        }

        complete(result.method);
      } catch (_) {
        status.textContent = "Sign-in could not be completed. Try again.";
      }
    });
    """
  end

  def setup_html do
    """
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <title>Set up a passkey · Harklight</title>
      #{DashboardBrand.favicon()}
      <style>
        :root { color-scheme: light dark; font: 1rem / 1.55 system-ui, sans-serif; }
        main { max-width: 25rem; margin: 11vh auto; padding: 1.5rem; }
        button { font: inherit; padding: .45rem .75rem; }
      </style>
    </head>
    <body>
      <main>
        #{DashboardBrand.lockup(decorative: true)}
        <h1>Set up a passkey</h1>
        <button id="create-passkey" type="button">Create passkey</button>
        <p id="sign-in-status" role="status"></p>
        <p><a href="/devices">Back to Devices</a></p>
      </main>
      <script src="/sign-in/passkey.js" defer></script>
    </body>
    </html>
    """
  end

  def passkey_script do
    ~S"""
    (() => {
      const registration = document.getElementById("create-passkey");
      const button = registration || document.getElementById("use-passkey");
      const status = document.getElementById("sign-in-status");

      const decode = value => {
        const base64 = value.replace(/-/g, "+").replace(/_/g, "/");
        return Uint8Array.from(atob(base64), byte => byte.charCodeAt(0));
      };

      const encode = buffer => {
        let bytes = "";
        for (const byte of new Uint8Array(buffer)) bytes += String.fromCharCode(byte);
        return btoa(bytes).replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
      };

      const post = async (path, body) => {
        const response = await fetch(path, {
          method: "POST",
          headers: {"content-type": "application/json"},
          body: JSON.stringify(body)
        });
        if (!response.ok) throw new Error("Passkey request was refused");
        return response.json();
      };

      if (button && !window.PublicKeyCredential) button.hidden = true;
      if (button) button.addEventListener("click", async () => {
        button.disabled = true;
        status.textContent = "";
        const path = registration ? "/devices/passkey" : "/sign-in/passkey";
        try {
          const {reference, publicKey} = await post(path + "/options", {});
          publicKey.challenge = decode(publicKey.challenge);

          let credential;
          if (registration) {
            publicKey.user.id = decode(publicKey.user.id);
            credential = await navigator.credentials.create({publicKey});
          } else {
            credential = await navigator.credentials.get({publicKey});
          }

          const payload = {
            reference,
            client_data: encode(credential.response.clientDataJSON)
          };
          if (registration) {
            payload.attestation = encode(credential.response.attestationObject);
          } else {
            payload.credential_id = encode(credential.rawId);
            payload.authenticator_data = encode(credential.response.authenticatorData);
            payload.signature = encode(credential.response.signature);
          }

          const result = await post(path + "/finish", payload);
          if (registration && result.registered) {
            status.textContent = "Passkey saved.";
          } else if (result.method === "passkey") {
            button.dispatchEvent(new CustomEvent("harklight-passkey-complete"));
          }
        } catch (_) {
          status.textContent = registration
            ? "Passkey setup could not be completed."
            : "Passkey sign-in could not be completed. You can use a one-time code.";
        } finally {
          button.disabled = false;
        }
      });
    })();
    """
  end

end
