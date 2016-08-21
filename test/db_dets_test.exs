defmodule Db.Dets.Test do
  use ExUnit.Case
  @behaviour Db
  doctest Db.Dets

  defstruct action: :undefined

  test "Write then lookup works" do
    :ok = Db.execute(%Db.Dets.Test{action: :kv_write}, {:foo1, :bar})
    assert {:ok, :bar} == Db.execute(%Db.Dets.Test{action: :kv_lookup}, :foo1)
  end

  test "Lookup non-existant works" do
    assert {:error, :notfound} == Db.execute(%Db.Dets.Test{action: :kv_lookup}, :baz)
  end

  test "Deleting non-existant works" do
    :ok = Db.execute(%Db.Dets.Test{action: :kv_delete}, :baz)
  end

  test "Write then delete works" do
    :ok = Db.execute(%Db.Dets.Test{action: :kv_write}, {:foo2, :bar})
    :ok = Db.execute(%Db.Dets.Test{action: :kv_delete}, :foo2)
    assert {:error, :notfound} == Db.execute(%Db.Dets.Test{action: :kv_lookup}, :foo2)
  end

  test "List insert works" do
    assert {:error, :notfound} == Db.execute(%Db.Dets.Test{action: :kv_lookup}, :foo3)
    :ok = Db.execute(%Db.Dets.Test{action: :kv_list_write}, {:foo3, 1})
    assert {:ok, [1]} == Db.execute(%Db.Dets.Test{action: :kv_lookup}, :foo3)
    # Inserting the same element twice only keeps one
    :ok = Db.execute(%Db.Dets.Test{action: :kv_list_write}, {:foo3, 1})
    assert {:ok, [1]} == Db.execute(%Db.Dets.Test{action: :kv_lookup}, :foo3)
    :ok = Db.execute(%Db.Dets.Test{action: :kv_list_write}, {:foo3, 2})
    assert {:ok, [2, 1]} == Db.execute(%Db.Dets.Test{action: :kv_lookup}, :foo3)
  end

  test "List delete works" do
    :ok = Db.execute(%Db.Dets.Test{action: :kv_list_write}, {:foo4, 1})
    :ok = Db.execute(%Db.Dets.Test{action: :kv_list_write}, {:foo4, 2})

    :ok = Db.execute(%Db.Dets.Test{action: :kv_list_delete}, {:foo4, 5})
    assert {:ok, [2, 1]} == Db.execute(%Db.Dets.Test{action: :kv_lookup}, :foo4)

    :ok = Db.execute(%Db.Dets.Test{action: :kv_list_delete}, {:foo4, 1})
    assert {:ok, [2]} == Db.execute(%Db.Dets.Test{action: :kv_lookup}, :foo4)

    :ok = Db.execute(%Db.Dets.Test{action: :kv_list_delete}, {:foo4, 2})
    assert {:ok, []} == Db.execute(%Db.Dets.Test{action: :kv_lookup}, :foo4)
  end

  def handle(%Db.Dets.Test{action: :kv_write}, {key, value}), do: %Db.Dets{keyspace: :test, query: {:insert, key, value}}
  def handle(%Db.Dets.Test{action: :kv_delete}, key), do: %Db.Dets{keyspace: :test, query: {:delete, key}}
  def handle(%Db.Dets.Test{action: :kv_lookup}, key), do: %Db.Dets{keyspace: :test, query: {:lookup, key}}

  def handle(%Db.Dets.Test{action: :kv_list_write}, {key, value}), do: %Db.Dets{keyspace: :test, query: {:insert_list, key, value}}
  def handle(%Db.Dets.Test{action: :kv_list_delete}, {key, value}), do: %Db.Dets{keyspace: :test, query: {:delete_list, key, value}}

  defimpl Db.Router, for: Db.Dets.Test do
    def route(_), do: %Db.Router{module: Db.Dets.Test, inline: true}
  end
end

