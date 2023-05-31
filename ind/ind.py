#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import duckdb
import typing as t
from pathlib import Path


def display_trains(staff: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Отобразить список поездов.
    """
    # Проверить, что список поездов не пуст.
    if staff:
        # Заголовок таблицы.
        line = '+-{}-+-{}-+-{}-+-{}-+'.format(
            '-' * 4,
            '-' * 30,
            '-' * 20,
            '-' * 15
        )
        print(line)
        print(
            '| {:^4} | {:^30} | {:^20} | {:^15} |'.format(
                "No",
                "Пункт назначения",
                "Номер поезда",
                "Тип поезда"
            )
        )
        print(line)

        # Вывести данные о всех поездах.
        for idx, train in enumerate(staff, 1):
            print(
                '| {:>4} | {:<30} | {:<20} | {:>15} |'.format(
                    idx,
                    train.get('destination', ''),
                    train.get('num', 0),
                    train.get('typ', '')
                )
            )
            print(line)

    else:
        print("Список поездов пуст.")


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    # Создать таблицу с информацией о типах.
    cursor.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS type_st START 1
        """
    )
    cursor.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS train_st START 1
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS types (
            type_id INTEGER PRIMARY KEY,
            type_title TEXT NOT NULL
        )
        """
    )

    # Создать таблицу с информацией о поездах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS trains (
            train_id INTEGER PRIMARY KEY,
            train_destination TEXT NOT NULL,
            type_id INTEGER NOT NULL,
            train_num INTEGER NOT NULL,
            FOREIGN KEY(type_id) REFERENCES types(type_id)
        )
        """
    )

    conn.close()


def add_train(
    database_path: Path,
    destination: str,
    typ: str,
    num: int
) -> None:
    """
    Добавить поезд в базу данных.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()
    # Получить идентификатор типа поезда в базе данных.
    # Если такой записи нет, то добавить информацию о новом типе.
    cursor.execute(
        """
        SELECT type_id FROM types WHERE type_title = ?
        """,
        (typ,)
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO types VALUES (nextval('type_st'), ?)
            """,
            (typ,)
        )
        cursor.execute(
            """
            SELECT currval('type_st')
            """
        )
        sel = cursor.fetchone()
        type_id = sel[0]

    else:
        type_id = row[0]

    # Добавить информацию о новом поезде.
    cursor.execute(
        """
        INSERT INTO trains (train_id, train_destination, type_id, train_num)
        VALUES (nextval('train_st'), ?, ?, ?)
        """,
        (destination, type_id, num)
    )

    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать все поезда.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT trains.train_destination, types.type_title, trains.train_num
        FROM trains
        INNER JOIN types ON types.type_id = trains.type_id
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
        "destination": row[0],
        "typ": row[1],
        "num": row[2],
        }
        for row in rows
    ]


def select_by_type(
    database_path: Path, train_type: str
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать поезда с заданным типом.
    """

    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT trains.train_destination, types.type_title, trains.train_num
        FROM trains
        INNER JOIN types ON types.type_id = trains.type_id
        WHERE types.type_title = ?
        """,
        (train_type,)
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "destination": row[0],
            "typ": row[1],
            "num": row[2],
        }
        for row in rows
    ]


def main(command_line=None):
    # Создать родительский парсер для определения имени файла.
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        default=str(Path.cwd() / "trains.db"),
        help="The database file name"
    )

    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("trains")
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0"
    )
    subparsers = parser.add_subparsers(dest="command")

    # Создать субпарсер для добавления поезда.
    add = subparsers.add_parser(
        "add",
        parents=[file_parser],
        help="Add a new train"
    )
    add.add_argument(
        "-d",
        "--destination",
        action="store",
        required=True,
        help="The train's destination"
    )
    add.add_argument(
        "-n",
        "--num",
        action="store",
        type=int,
        required=True,
        help="The train's number"
    )
    add.add_argument(
        "-t",
        "--typ",
        action="store",
        required=True,
        help="The train's type"
    )

    # Создать субпарсер для отображения всех поездов.
    _ = subparsers.add_parser(
        "display",
        parents=[file_parser],
        help="Display all trains"
    )

    # Создать субпарсер для выбора поездов.
    select = subparsers.add_parser(
        "select",
        parents=[file_parser],
        help="Select the trains"
    )
    select.add_argument(
        "-T",
        "--type",
        action="store",
        required=True,
        help="The required type"
    )

    # Выполнить разбор аргументов командной строки.
    args = parser.parse_args(command_line)

    # Получить путь к файлу базы данных.
    db_path = Path(args.db)
    create_db(db_path)

    # Добавить поезд.
    if args.command == "add":
        add_train(db_path, args.destination, args.typ, args.num)

    # Отобразить все поезда.
    elif args.command == "display":
        display_trains(select_all(db_path))

    # Выбрать требуемые поезда.
    elif args.command == "select":
        display_trains(select_by_type(db_path, args.type))
    pass


if __name__ == "__main__":
    main()