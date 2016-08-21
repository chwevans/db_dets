# DbDets

A plugin for DB implementing a connector to DETS.

This implements Db.Backend defined in https://github.com/chwevans/db.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add db_dets to your list of dependencies in `mix.exs`:

        def deps do
          [{:db_dets, "~> 0.0.1"}]
        end

  2. Ensure db_dets is started before your application:

        def application do
          [applications: [:db_dets]]
        end

