defmodule AttentionServer.DashboardBrand do
  @moduledoc false

  # Embed these fixed assets so the same markup works behind either dashboard
  # listener without adding an unauthenticated route or a network dependency.
  @lockup_path Path.expand("../../priv/dashboard/brand/harklight-navy.svg", __DIR__)
  @favicon_path Path.expand("../../priv/dashboard/brand/favicon.svg", __DIR__)
  @external_resource @lockup_path
  @external_resource @favicon_path
  @lockup_uri "data:image/svg+xml;base64," <> Base.encode64(File.read!(@lockup_path))
  @favicon_uri "data:image/svg+xml;base64," <> Base.encode64(File.read!(@favicon_path))

  def favicon do
    ~s(<link rel="icon" type="image/svg+xml" href="#{@favicon_uri}">)
  end

  def lockup(opts \\ []) do
    alt = if Keyword.get(opts, :decorative, false), do: "", else: "Harklight"

    ~s(<img src="#{@lockup_uri}" alt="#{alt}" width="160" height="38" style="display:block;max-width:100%;height:auto">)
  end
end
