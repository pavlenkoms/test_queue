defmodule TestQueue.Core do
  use GenServer
  alias :dets, as: Dets
  alias :queue, as: Queue
  alias :timer, as: Timer


  @timeout Application.get_env(:test_queue, __MODULE__, 5000)[:timeout]
  @folder Application.get_env(:test_queue, __MODULE__, "dev_dets/")[:folder]


  def start_link(name) do
    case GenServer.start_link(__MODULE__, [name]) do
      {:ok, pid} = res -> res
      _ -> :error
    end
  end

  def add(queue, msg) do
    GenServer.call(queue, {:add, msg})
  end

  def get(queue, timeout) do
    GenServer.call(queue, :get, timeout)
  end

  def ack(queue, uuid) do
    GenServer.call(queue, {:ack, uuid})
  end

  def reject(queue, uuid) do
    GenServer.call(queue, {:reject, uuid})
  end

  def drop(queue) do
    GenServer.call(queue, :drop)
  end

  def stop(queue) do
    GenServer.stop(queue)
  end

  def init([name]) do
    GenServer.cast(self(), :go)
    File.mkdir_p!(@folder)
    filename = [@folder, Atom.to_string(name)] |> Path.join() |> String.to_charlist()
    {:ok, queue_table} = Dets.open_file(name, [file: filename, ram_file: true, auto_save: :infinity])

    {:ok, %{table: queue_table, queue: nil, client_queue: Queue.new()}}
  end

  def handle_cast(:go, %{table: table} = state) do
    queue = case state.table |> Dets.lookup(:queue) do
      [] -> Queue.new()
      [{:queue, q}] -> q
    end

    to_resurrect = Dets.foldl(&preprocess_table/2, [], table)

    to_resurrect
    |> Enum.each(fn {uuid, msg} ->
      {:ok, ref} = Timer.send_after(@timeout, {:timeout, uuid})

      msg =
        msg
        |> Map.put(:to_ref, ref)
        |> Map.put(:state, :in_process)

      Dets.insert(table, {uuid, msg})
    end)

    :ok = Dets.sync(table)
    {:noreply, %{state | queue: queue}}
  end

  def handle_call(:get, from, %{queue: queue, table: table} = state) do
    with false <- Queue.is_empty(queue),
         {{:value, uuid}, queue} <- Queue.out(queue),
         [{uuid, msg}] <- Dets.lookup(table, uuid) do

      {:ok, ref} = Timer.send_after(@timeout, {:timeout, uuid})

      msg =
        msg
        |> Map.put(:to_ref, ref)
        |> Map.put(:state, :in_process)

      Dets.insert(table, {uuid, msg})
      Dets.insert(table, {:queue, queue})

      :ok = Dets.sync(table)

      {:reply, {:ok, {uuid, msg.body}}, %{state | queue: queue}}
    else
      true ->
        client_queue = state.client_queue
        client_queue = Queue.in(from, client_queue)
        {:noreply, %{state | client_queue: client_queue}}
      [] ->
        # непонятный айдишник в очереди. Дропаем, пробуем снова
        # TODO: log error
        {_, queue} = Queue.out(queue)

        Dets.insert(table, {:queue, queue})
        :ok = Dets.sync(table)

        handle_call(:get, from, %{state | queue: queue})
    end
  end

  def handle_call({:add, message}, _, %{table: table} = state) do
    uuid = UUID.uuid4() #Тащить экто ради одного ууида - показалось как то оверкилл

    state =
      case to_queue_or_client(uuid, message, state) do
        {:to_queue, state} ->                          # не обернул в функцию
          :ok = Dets.sync(table)                       # что бы sync был явно виден
          state
        {:to_client, client, state} ->
          :ok = Dets.sync(table)
          GenServer.reply(client, {:ok, {uuid, message}})
          state
      end
    {:reply, :ok, state}
  end

  def handle_call({:ack, uuid}, _, %{table: table} = state) do
    case Dets.lookup(table, uuid) do
      [{_uuid, %{state: :in_process, to_ref: to_ref}}] ->
        to_ref && Timer.cancel(to_ref)

        Dets.delete(table, uuid)

        :ok = Dets.sync(table)

        {:reply, :ok, state}
      [{_uuid, _msg}] ->
        # время покажет насколько нужно знать пользователю, что этот уид в системе есть
        # но под это действие он не подходит... Но у себя ошибочку сделаем отдельную
        # TODO: log error
        {:reply, {:error, :bad_uuid}, state}
      [] ->
        # TODO: log error
        {:reply, {:error, :bad_uuid}, state}
    end
  end

  def handle_call({:reject, uuid}, _, %{table: table} = state) do
    case Dets.lookup(table, uuid) do
      [{uuid, %{state: :in_process, to_ref: to_ref} = msg}] ->
        to_ref && Timer.cancel(to_ref)
        message = msg.body

        state =
          case to_queue_or_client(uuid, message, state) do
            {:to_queue, state} ->
              :ok = Dets.sync(table)
              state
            {:to_client, client, state} ->
              :ok = Dets.sync(table)
              GenServer.reply(client, {:ok, {uuid, message}})
              state
          end

        {:reply, :ok, state}
      [{_uuid, _msg}] ->
        # время покажет насколько нужно знать пользователю, что этот уид в системе есть
        # но под это действие он не подходит... Но у себя ошибочку сделаем отдельную
        # TODO: log error
        {:reply, {:error, :bad_uuid}, state}
      [] ->
        # TODO: log error
        {:reply, {:error, :bad_uuid}, state}
    end
  end

  def handle_call(:drop, _, state) do
    state.table |> Dets.delete_all_objects()
    Dets.sync(state.table)
    {:reply, :ok, %{state | queue: Queue.new(), client_queue: Queue.new()}}
  end

  def handle_info({:timeout, uuid}, %{table: table} = state) do
    case Dets.lookup(table, uuid) do
      [{uuid, msg}] ->
        message = msg.body

        state =
          case to_queue_or_client(uuid, message, state) do
            {:to_queue, state} ->
              :ok = Dets.sync(table)
              state
            {:to_client, client, state} ->
              :ok = Dets.sync(table)
              GenServer.reply(client, {:ok, {uuid, message}})
              state
          end

        {:noreply, state}
      [] ->
        # TODO: log error
        {:noreply, state}
    end
  end

  def terminate(_, state) do
    Dets.close(state.table)
    :ok
  end

  defp preprocess_table({:queue, _}, acc), do: acc

  defp preprocess_table({_uuid, %{state: :in_process}} = message, acc) do
    [message | acc]
  end

  defp preprocess_table({_uuid, _}, acc) do
    acc
  end

  defp to_queue_or_client(uuid, message, %{client_queue: client_queue, table: table} = state) do
    if Queue.is_empty(client_queue) do
      msg = %{
        to_ref: nil,
        state: :queued,
        body: message
      }

      queue = state.queue
      queue = Queue.in(uuid, queue)
      Dets.insert(table, {:queue, queue})
      Dets.insert(table, {uuid, msg})

      {:to_queue, %{state | queue: queue}}
    else
      {{:value, client}, client_queue} = Queue.out(client_queue)
      {:ok, ref} = Timer.send_after(@timeout, {:timeout, uuid})

      msg = %{
        to_ref: ref,
        state: :in_process,
        body: message
      }

      Dets.insert(table, {uuid, msg})

      {:to_client, client, %{state | client_queue: client_queue}}
    end
  end
end