defmodule TestQueueTest do
  use ExUnit.Case
  doctest TestQueue

  @folder Application.get_env(:test_queue, TestQueue.Core, "test_dets/")[:folder]

  defp worker(pid, id, que) do
    receive do
      :make_get ->
        {:ok, {uuid, msg}} = TestQueue.get(que)
        send(pid, {id, {uuid, msg}})
        worker(pid, id, que)
      {:make_ack, uuid} ->
        :ok = TestQueue.ack(que, uuid)
        send(pid, {id, :acked})
        worker(pid, id, que)
      {:make_reject, uuid} ->
        :ok = TestQueue.reject(que, uuid)
        send(pid, {id, :rejected})
        worker(pid, id, que)
      :stop -> :ok
    end
  end

  test "simple get" do
    que_name = :test1
    {:ok, que} = TestQueue.create(que_name)

    TestQueue.drop(que)

    :ok = TestQueue.add(que, :msg1)
    {:ok, {uuid, msg}} = TestQueue.get(que)
    assert msg == :msg1

    :ok = TestQueue.ack(que, uuid)

    :ok = TestQueue.stop(que)
    [@folder, Atom.to_string(que_name)] |> Path.join() |> File.rm()
  end

  test "double get" do
    que_name = :test2
    {:ok, que} = TestQueue.create(que_name)

    TestQueue.drop(que)

    :ok = TestQueue.add(que, :msg1)
    :ok = TestQueue.add(que, :msg2)

    {:ok, {uuid, msg}} = TestQueue.get(que)
    assert msg == :msg1

    :ok = TestQueue.ack(que, uuid)

    {:ok, {uuid, msg}} = TestQueue.get(que)
    assert msg == :msg2

    :ok = TestQueue.ack(que, uuid)


    :ok = TestQueue.stop(que)
    [@folder, Atom.to_string(que_name)] |> Path.join() |> File.rm()
  end

  test "double get without ack" do
    que_name = :test3
    {:ok, que} = TestQueue.create(que_name)

    TestQueue.drop(que)

    :ok = TestQueue.add(que, :msg1)
    :ok = TestQueue.add(que, :msg2)

    {:ok, {uuid1, msg1}} = TestQueue.get(que)
    assert msg1 == :msg1

    {:ok, {uuid2, msg2}} = TestQueue.get(que)
    assert msg2 == :msg2

    :ok = TestQueue.ack(que, uuid1)
    :ok = TestQueue.ack(que, uuid2)

    :ok = TestQueue.stop(que)
    [@folder, Atom.to_string(que_name)] |> Path.join() |> File.rm()
  end

  test "get and reject" do
    que_name = :test4

    {:ok, que} = TestQueue.create(que_name)

    TestQueue.drop(que)

    :ok = TestQueue.add(que, :msg1)
    :ok = TestQueue.add(que, :msg2)

    {:ok, {uuid, msg}} = TestQueue.get(que)
    assert msg == :msg1

    :ok = TestQueue.reject(que, uuid)

    {:ok, {uuid1, msg1}} = TestQueue.get(que)
    assert msg1 == :msg2
    {:ok, {uuid2, msg2}} = TestQueue.get(que)
    assert msg2 == :msg1

    :ok = TestQueue.ack(que, uuid1)
    :ok = TestQueue.ack(que, uuid2)

    :ok = TestQueue.stop(que)
    [@folder, Atom.to_string(que_name)] |> Path.join() |> File.rm()
  end

  test "concurent get" do
    pid = self()

    que_name = :test5
    {:ok, que} = TestQueue.create(que_name)

    TestQueue.drop(que)

    worker1 = spawn_link(fn -> worker(pid, :wrk1, que) end)
    worker2 = spawn_link(fn -> worker(pid, :wrk2, que) end)


    :ok = TestQueue.add(que, :msg1)
    :ok = TestQueue.add(que, :msg2)
    :ok = TestQueue.add(que, :msg3)
    :ok = TestQueue.add(que, :msg4)

    send(worker1, :make_get)
    assert_receive({:wrk1, {uuid1, :msg1}}, 1000)

    send(worker2, :make_get)
    assert_receive({:wrk2, {uuid2, :msg2}}, 1000)

    send(worker2, {:make_ack, uuid2})
    assert_receive({:wrk2, :acked}, 1000)

    send(worker2, :make_get)
    assert_receive({:wrk2, {uuid3, :msg3}}, 1000)

    send(worker1, {:make_ack, uuid1})
    assert_receive({:wrk1, :acked}, 1000)

    send(worker1, :make_get)
    assert_receive({:wrk1, {uuid4, :msg4}}, 1000)

    send(worker1, {:make_ack, uuid4})
    assert_receive({:wrk1, :acked}, 1000)

    send(worker2, {:make_ack, uuid3})
    assert_receive({:wrk2, :acked}, 1000)

    :ok = TestQueue.stop(que)
    [@folder, Atom.to_string(que_name)] |> Path.join() |> File.rm()
  end

  test "persistence test" do
    que_name = :test6
    {:ok, que} = TestQueue.create(que_name)

    TestQueue.drop(que)

    :ok = TestQueue.add(que, :msg1)
    :ok = TestQueue.add(que, :msg2)

    {:ok, {uuid, msg}} = TestQueue.get(que)
    assert msg == :msg1

    :ok = TestQueue.ack(que, uuid)

    :ok = TestQueue.stop(que)

    {:ok, que} = TestQueue.create(que_name)

    {:ok, {uuid, msg}} = TestQueue.get(que)
    assert msg == :msg2

    :ok = TestQueue.ack(que, uuid)

    :ok = TestQueue.stop(que)
    [@folder, Atom.to_string(que_name)] |> Path.join() |> File.rm()
  end

  test "bad uuid ack" do
    que_name = :test7
    {:ok, que} = TestQueue.create(que_name)

    TestQueue.drop(que)

    :ok = TestQueue.add(que, :msg1)
    :ok = TestQueue.add(que, :msg2)

    {:ok, {uuid, msg}} = TestQueue.get(que)
    assert msg == :msg1

    {:error, :bad_uuid} = TestQueue.ack(que, "some_shit")

    :ok = TestQueue.ack(que, uuid)

    {:ok, {uuid, msg}} = TestQueue.get(que)
    assert msg == :msg2

    :ok = TestQueue.ack(que, uuid)

    :ok = TestQueue.stop(que)
    [@folder, Atom.to_string(que_name)] |> Path.join() |> File.rm()
  end

  test "bad uuid reject" do
    que_name = :test8
    {:ok, que} = TestQueue.create(que_name)

    TestQueue.drop(que)

    :ok = TestQueue.add(que, :msg1)
    :ok = TestQueue.add(que, :msg2)

    {:ok, {uuid, msg}} = TestQueue.get(que)
    assert msg == :msg1

    {:error, :bad_uuid} = TestQueue.reject(que, "some_shit")

    :ok = TestQueue.reject(que, uuid)

    {:ok, {uuid, msg}} = TestQueue.get(que)
    assert msg == :msg2

    :ok = TestQueue.ack(que, uuid)

    :ok = TestQueue.stop(que)
    [@folder, Atom.to_string(que_name)] |> Path.join() |> File.rm()
  end

  test "answer to timeouted msg" do
    que_name = :test9
    {:ok, que} = TestQueue.create(que_name)

    TestQueue.drop(que)

    :ok = TestQueue.add(que, :msg1)
    :ok = TestQueue.add(que, :msg2)

    {:ok, {fucked_up_uuid, msg}} = TestQueue.get(que)
    assert msg == :msg1

    :timer.sleep(1100)

    {:error, :bad_uuid} = TestQueue.ack(que, fucked_up_uuid)

    {:ok, {uuid, msg}} = TestQueue.get(que)
    assert msg == :msg2

    :ok = TestQueue.ack(que, uuid)

    {:ok, {^fucked_up_uuid, msg}} = TestQueue.get(que)
    assert msg == :msg1

    :ok = TestQueue.ack(que, fucked_up_uuid)

    :ok = TestQueue.stop(que)
    [@folder, Atom.to_string(que_name)] |> Path.join() |> File.rm()
  end

end
