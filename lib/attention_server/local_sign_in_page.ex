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
          <div class="preferences">
            <label><input type="checkbox" id="remember-method"> Remember my sign-in preference on this device</label>
            <button type="button" id="use-preference">Use my remembered preference</button>
          </div>
        </form>
        <p id="sign-in-status" role="status"></p>
        <noscript>Enable JavaScript to sign in.</noscript>
      </main>
      <script src="/sign-in/code.js" defer></script>
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
    const storageKey = "harklight.last-sign-in-method";
    const params = new URLSearchParams(location.search);
    const appReturn = params.get("return") === "app";
    const transaction = params.get("transaction");

    if (appReturn) document.querySelector(".preferences").hidden = true;

    preference.addEventListener("click", () => {
      try {
        marker.hidden = localStorage.getItem(storageKey) !== "owner_code";
      } catch (_) {
        marker.hidden = true;
      }
    });

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

        try {
          if (remember.checked && result.method === "owner_code") {
            localStorage.setItem(storageKey, result.method);
          } else {
            localStorage.removeItem(storageKey);
          }
        } catch (_) {
          // The preference is optional in a profile with storage disabled.
        }
        location.replace("/inbox");
      } catch (_) {
        status.textContent = "Sign-in could not be completed. Try again.";
      }
    });
    """
  end
end
