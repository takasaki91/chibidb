# はじめに

勉強のために作成しました。

固定テーブルにinsertとselectしかできません。

# 動かし方

```sh
$ go run main.go
```

```
chibidb > insert 1 user1 person1@example.com
chibidb > select
(1, user1, person1@example.com)
chibidb >.exit
```
