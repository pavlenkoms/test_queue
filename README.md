# TestQueue

**Что делать?**

Качаем с гита

делаем `mix deps.get && iex -S mix`

**Что там есть?**

`TestQueue.create/1` - для старта очереди

`TestQueue.add/2` - для покладки сообщения в очередь

`TestQueue.get/2` - для получения сообщения из очереди

`TestQueue.ack/2` - что бы подтвердить обработку

`TestQueue.reject/2` - вернуть сообщение обратно в очередь

а так же

`TestQueue.drop/1` - опустошить очередь

`TestQueue.stop/1` - остановить работу очереди
