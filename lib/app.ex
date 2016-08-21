defmodule Db.Dets.App do
  require Logger

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      worker(Db.Dets, [], [id: Db.Dets, function: :start_link, shutdown: :brutal_kill]),
    ]

    Logger.info("Starting #{inspect __MODULE__} with children: #{inspect children}")

    options = [strategy: :one_for_one, name: __MODULE__]
    Supervisor.start_link(children, options)
  end
end
