# ITMO-Distributed-Systems-y2022

## Preface

Репозиторий представляет из себя выполненные лично домашние задания курса Программирование распределенных систем.

Проект создан исключительно для отражения учебной деятельности на 3 курсе. Автор не несет ответственности за 
действия людей, решивших за чужой счет сдать задания, предоставив код из данного репозитория.

Код, написанный мной, находится в файлах папки `src`.

Для проверки решений преподаватель данного курса предоставил тесты, которые находятся в папке `test`.

К каждому заданию есть и описание, и инструкция по запуску тестов (см. `README.md`).

Приятного погружения в мир распределенного программирования!

## Tasks

1. [Distributed Mutex](distributed-mutex) 💥: алгоритм обедающих философов распределенной взаимной блокировки, 
посылающий количество сообщений, пропорциональное количеству процессов, которые хотят попасть в критическую секцию. 
2. [Distributed FIFO Order](distributed-fifo): в некоторых распределенных алгоритмах требуются гарантии на порядок
посылаемых сообщений. В этом задании воссоздается ситуация, при котороый алгоритм взаимной блокировки Лампорта работает
некорректно, если порядок сообщений **не** FIFO.
3. [Distributed Dijkstra](distributed-dijkstra): распределенный алгоритм Дейкстры для поиска кратчайшего расстояния 
пути от инициатора до остальных узлов.
4. [Consistent Hashing](consistent-hashing): алгоритмы добавления / удаления узла и поиска узла по ключу в консистентном 
хешировании, применяемые при решении задачи шардирования.
5. [Distributed Consensus Error](distributed-consensus-error): теорема FLP утверждает о невозможности консенсуса в 
асинхронной системе с отказом узла &mdash в этом задании нужно выполнить роль сети передачи данных, и подобрать 
такое исполнение, при котором некорректный алгоритм консенсуса не приходит к согласию, то есть разные процессы 
приходят к разному решению.
6. [Distributed Merge](distributed-merge): алгоритм распределённого слияния `N` отсортированных последовательностей.
7. [Raft](distributed-raft) 💥: алгоритм Raft для распределенного консенсуса.
