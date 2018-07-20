defmodule TestQueue do
  alias TestQueue.Core

  @spec create(name :: atom) :: {:ok, pid} | :error
  def create(name) do
    Core.start_link(name)
  end

  @spec add(queue :: pid, msg :: term) :: :ok
  def add(queue, msg) do
    Core.add(queue, msg)
  end

  @spec get(queue :: pid, timeout :: integer | :infinity) :: :ok
  def get(queue, timeout \\ :infinity) do
    Core.get(queue, timeout)
  end

  @spec ack(queue :: pid, uuid :: bitstring) :: :ok
  def ack(queue, uuid) do
    Core.ack(queue, uuid)
  end

  @spec reject(queue :: pid, uuid :: bitstring) :: :ok
  def reject(queue, uuid) do
    Core.reject(queue, uuid)
  end

  # Несколько команд для удобства тестирования

  @spec drop(queue :: pid) :: :ok
  def drop(queue) do
    Core.drop(queue)
  end

  @spec stop(queue :: pid) :: :ok
  def stop(queue) do
    Core.stop(queue)
  end
end
