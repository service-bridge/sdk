# Quickstart

[← к индексу](./index.md)

Два процесса, один типизированный вызов между ними. Пять минут.

## Предусловия

- Go 1.24+.
- Запущенный рантайм ServiceBridge. Если его нет:

  ```sh
  bash <(curl -fsSL https://servicebridge.dev/install.sh)
  ```

  Скрипт поднимает контейнер рантайма, подключает PostgreSQL 18+ и открывает gRPC control plane на `:14445`, дашборд — на `:14444`.

## 1. Установка

```sh
go get github.com/service-bridge/sdk/go
```

## 2. Bootstrap-ключи

Откройте дашборд `http://localhost:14444` → **Services → Create service**. Создайте два сервиса: `payment-svc` и `orders-svc`. У каждого скопируйте **bootstrap service key** — строку вида `sb.…`.

Ключ несёт в себе сертификат CA, поэтому SDK доверяет ровно одному корню и ничему из системного хранилища. Это второй аргумент `sb.New`.

```sh
# .env — в .gitignore
PAYMENT_KEY=sb....
ORDERS_KEY=sb....
```

SDK не читает переменные окружения сам: значения передаёте вы.

## 3. Общий контракт

Сообщения описываются в `.proto`, типы генерируются обычным способом.

```proto
// payment.proto
syntax = "proto3";
package demo.payment;
option go_package = "example.com/orders/paymentpb";

message ChargeRequest { string user_id = 1; int64 amount = 2; string currency = 3; }
message ChargeReply   { bool ok = 1; string transaction_id = 2; }
```

```sh
protoc -I . --go_out=. --go_opt=module=example.com/orders payment.proto
```

Блок `service` не нужен, `protoc-gen-go-grpc` не нужен: маршрутизацию делает рантайм, а метод именуется той строкой, под которой вы его регистрируете. Значение имеют только сообщения.

## 4. Callee — принимает вызовы

```go
package main

import (
	"context"
	"log"
	"os"

	"example.com/orders/paymentpb"
	sb "github.com/service-bridge/sdk/go"
)

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("PAYMENT_KEY"),
		sb.WithAdvertise("127.0.0.1", 50051))
	if err != nil {
		log.Fatal(err)
	}

	err = sb.Handle(c, "Charge",
		func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
			return &paymentpb.ChargeReply{
				Ok:            req.GetAmount() > 0,
				TransactionId: "tx-" + req.GetUserId(),
			}, nil
		})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	select {}
}
```

`sb.Handle` — свободная функция, а не метод клиента: в Go нет генерик-методов, а параметры типа здесь и есть контракт. Оба параметра выводятся из переданной функции, писать их руками не нужно.

## 5. Caller — типизированный вызов

```go
package main

import (
	"context"
	"log"
	"os"

	"example.com/orders/paymentpb"
	sb "github.com/service-bridge/sdk/go"
)

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("ORDERS_KEY"), sb.WithCallerOnly())
	if err != nil {
		log.Fatal(err)
	}

	payment := sb.NewClient(c, "payment-svc")
	charge, err := sb.NewMethod[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](payment, "Charge")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	res, err := charge.Call(ctx, &paymentpb.ChargeRequest{UserId: "u-1", Amount: 100})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("charged:", res.GetOk(), res.GetTransactionId())
}
```

`payment-svc` — это **имя сервиса** из дашборда, не хост и не порт. Кто именно обслужит вызов, решают реестр и балансировщик.

`sb.NewMethod` — это и есть всё объявление зависимости: он записывает исходящую зависимость на `payment-svc.Charge` и связывает схему из параметров типа. Второго шага «загрузить схему», который можно забыть, здесь нет.

## 6. Запуск

```sh
# первый терминал
PAYMENT_KEY=$(grep PAYMENT_KEY .env | cut -d= -f2) go run ./callee

# второй терминал
ORDERS_KEY=$(grep ORDERS_KEY .env | cut -d= -f2) go run ./caller
```

Во втором терминале появится `charged: true tx-u-1`. В дашборде на `:14444` — оба сервиса на карте и трейс вызова.

## Что произошло под капотом

1. `sb.New` только проверил конфигурацию и разобрал ключ. Сети здесь ещё не было: любая неверная граница отваливается сразу, с кодом `CodeConfig`, и не попадает в лестницу переподключения.
2. `sb.Handle` и `sb.NewMethod` положили объявления в набор, который поедет в первой регистрации.
3. `c.Start` запечатал объявления, открыл локальный outbox, провижинил mTLS-сертификат из bootstrap-ключа, поднял входящий сервер на объявленном адресе, зарегистрировался, дождался первого снапшота реестра и только после этого поднял подписки.
4. `charge.Call` выбрал живой инстанс `payment-svc`, публикующий ровно тот хеш контракта, который выведен из пары типов, и сходил в него напрямую по mTLS.

## Главное правило

**Всё объявляется до `Start`, вызывается — после.**

Обработчики, зависимости, события, задачи и workflow едут в первой регистрации. После `Start` набор запечатан: объявленный позже обработчик существовал бы в вашем процессе и нигде в mesh. Позднее объявление возвращает `CodeState`, а не проваливается молча.

## Дальше

- [RPC](./rpc.md) — стриминг, транспорт, ретраи, идемпотентность.
- [Events](./events.md) — durable-публикация и подписки.
- [Operations](./operations.md) — опции конструктора и lifecycle целиком.
