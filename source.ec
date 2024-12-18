#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

/* Объявление переменных для подключения к БД */
exec SQL begin declare section;
    char db_name[50];      /* Имя базы данных */
    char user[50];         /* Логин */
    char password[50];     /* Пароль */
exec SQL end declare section;

void ConnectDB() 
{

      strcpy(db_name, "students"); // Имя базы данных
      strcpy(user, "pmi-b1408"); // Логин
      strcpy(password, "Lokdiew4$"); // Пароль
      printf("Connecting to db \"%s\"...\n", db_name);
      exec SQL connect to :db_name user :user using :password;
      if (sqlca.sqlcode < 0)
      {
         printf("connect error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
         return;
      }
      printf("Success! code %d\n", sqlca.sqlcode);
      printf("Connecting to schema \"pmib1408\"...\n");
      exec sql set search_path to pmib1613;
      if (sqlca.sqlcode < 0)
      {
         printf("connect error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
         return;
      }
      printf("Success! code %d\n", sqlca.sqlcode);
      return;
}

void DisconnectDB()
{
   printf("Disconnecting from db \"%s\"...\n", db_name);
   exec SQL disconnect :db_name;
   if (sqlca.sqlcode < 0)
   {
      printf("disconnect error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      return;
   }
   printf("Success! code %d\n", sqlca.sqlcode);
   return;
}

void PrintMenu()
{
   printf("1) Task1\n");
   printf("2) Task2\n");
   printf("3) Task3\n");
   printf("4) Task4\n");
   printf("5) Task5\n");
   printf("6) Stop the program\n");
}

void Task1()
{
   /*
   1. Выдать число изделий, для которых детали с весом больше 12
      поставлял первый по алфавиту поставщик.
   */
   exec sql begin declare section;
      int count; // Результат запроса - число изделий
   exec sql end declare section;
   printf("Starting Task1 request processing...\n");
   exec sql begin work; //начало транзакции
   exec sql select count(distinct spj.n_izd)
            from spj
            inner join s on s.n_post=spj.n_post
            inner join p on p.n_det=spj.n_det
            where s.name=(select min(name) from s) and p.ves>12
            )
   if (sqlca.sqlcode < 0) //проверка кода возврата запроса
   {
      printf("Task1 error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work; // отмена всех изменений в рамках транзакции
      return;
   }
   else // если успешно завершено
   {
      printf("Success! code %d\n", sqlca.sqlcode);
      printf("Count: %d\n", count);
      exec sql commit work; // конец транзакции
      return;
   }
}

void Task2()
{
   /*
   2. Поменять местами фамилии первого и последнего по алфавиту
      поставщика, т. е. первому по алфавиту поставщику установить фами-
      лию последнего по алфавиту поставщика и наоборот.
   */
   printf("Starting Task2 request processing...\n");
   exec sql begin work; //начало транзакци
   exec UPDATE s set name = (CASE WHEN name = (SELECT min(name)
                                       FROM s)
                          THEN (SELECT max(name)
                                FROM s)
                          ELSE (SELECT min(name)
                                FROM s) 
                          END)
                          WHERE name = (SELECT min(name)
                          FROM s)
                          or
                          name = (SELECT max(name)
                          FROM s)
   if (sqlca.sqlcode < 0)
   {
      printf("Task2 error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work;
      return;
   }
   if (sqlca.sqlcode == 100) //проверка на отсутствие данных
   {
      printf("There is no data to update!\n");
      return;
   }   
   if (sqlca.sqlcode == 0)
   {
      printf("Success! code %d\n", sqlca.sqlcode);
      printf("Changes made: %d\n", sqlca.sqlerrd[2]);
      exec sql commit work; // конец транзакции
      return;
   }
}


void Task3()
{
   /*
   3. Найти изделия, для которых выполнены поставки, вес которых
      более чем в 4 раза превышает минимальный вес поставки для изделия.
      Вывести номер изделия, вес поставки, минимальный вес поставки для
      изделия.
   */
   exec sql begin declare section;
      char n_izd[6]; // Результат запроса - номера деталей
   exec sql end declare section;
   printf("Starting Task3 request processing...\n");
   // объявление курсора
   exec sql declare curs1 cursor for
      select a.n_izd, a.kol*pa.ves pves, b.mves
      from spj a
      join p pa on pa.n_det=a.n_det
      join (select t.n_izd, min(t.kol*p.ves) mves
            from spj t
            join p on p.n_det=t.n_det
            group by t.n_izd
            ) b on b.n_izd=a.n_izd
      where a.kol*pa.ves>b.mves*4
      order by 1,2
   if (sqlca.sqlcode < 0) // проверка объявления
   {
      printf("declare error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work;
      return;
   }
   exec sql begin work; //начало транзакци
   exec sql open curs1;   // открываем курсор
   if (sqlca.sqlcode < 0) // проверка открытия
   {
      printf("open error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs1;
      exec sql rollback work;
      return;
   }
   exec sql fetch curs1; // следующая строка из активного множества
   if (sqlca.sqlcode < 0) 
   {
      printf("fetch error! %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc); 
      exec sql close curs1;
      exec sql rollback work;
      return;
   }
   if (sqlca.sqlcode == 100)
   {
      printf("No results found\n");
      exec sql commit work;
      return;
   }
   int r_count = 1;
   printf("n_izd\n");
   printf("%s\n", n_izd);
   while (sqlca.sqlcode == 0) // Пока не дошли до конца активного множества
   {
      exec sql fetch curs1; // следующая строка из активного множества
      if (sqlca.sqlcode == 0)
      {
         printf("%s\n", n_izd);
         r_count += 1;
      }
   }
   if (sqlca.sqlcode == 100)
   {
      exec sql close curs1; // закрытие курсора
      printf("Success!\n");
      printf("Rows processed: %d\n", r_count);
      exec sql commit work;
      return;
   }
   if (sqlca.sqlcode < 0)
   {
      printf("fetch error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc); 
      exec sql close curs1;
      exec sql rollback work;
      return;
   }
}


void Task4()
{
   /*
   4. Выбрать поставщиков, не поставивших ни одной из деталей,
      имеющих наименьший вес.
   */
   exec sql begin declare section;
      char n_post[6]; // Результат запроса - номера поставщиков
   exec sql end declare section;
   printf("Starting Task4 request processing...\n");
   exec sql declare curs2 cursor for
      SELECT distinct spj.n_post
      FROM spj
      EXCEPT
      SELECT spj.n_post
      FROM spj
      WHERE spj.n_det in (SELECT n_det
                          FROM p
                          WHERE ves = (SELECT min(ves) FROM p))
      UNION
      SELECT DISTINCT n_post
      FROM s a
      WHERE NOT EXISTS(SELECT * 
                         FROM spj 
                         WHERE spj.n_post=a.n_post)
   if (sqlca.sqlcode < 0) // проверка объявления
   {
      printf("declare error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work;
      return;
   }
   exec sql begin work; //начало транзакци
   exec sql open curs2;   // открываем курсор
   if (sqlca.sqlcode < 0) // проверка открытия
   {
      printf("open error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs2;
      exec sql rollback work;
      return;
   }
   exec sql fetch curs2; // следующая строка из активного множества
   if (sqlca.sqlcode < 0) 
   {
      printf("fetch error! %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs2;
      exec sql rollback work; 
      return;
   }
   int r_count = 1;
   printf("n_post\n");
   printf("%s\n", n_post);
   while (sqlca.sqlcode == 0) // Пока не дошли до конца активного множества
   {
      exec sql fetch curs2; // следующая строка из активного множества
      if (sqlca.sqlcode == 0)
      {
         printf("%s\n", n_post);
         r_count += 1;
      }
   }
   if (sqlca.sqlcode == 100)
   {
      exec sql close curs2; // закрытие курсора
      printf("Success!\n");
      printf("Rows processed: %d\n", r_count);
      exec sql commit work;
      return;
   }
   if (sqlca.sqlcode < 0)
   {
      printf("fetch error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs2;
      exec sql rollback work; 
      return;
   }
}


void Task5()
{
   /*
   5. Выдать полную информацию о поставщиках, поставляющих
   ТОЛЬКО красные детали и только для изделия с длиной названия не
   меньше 7
   */
   exec sql begin declare section;
      char n_post[6], name[20], town[20];
      int reiting;
   exec sql end declare section;
   exec sql declare curs3 cursor for
      SELECT DISTINCT s.n_post,s.name,s.town, s.reiting
      FROM spj
      JOIN s ON s.n_post=spj.n_post
      WHERE n_det IN (SELECT n_det
                      FROM p
                      WHERE cvet='Красный') 
                      and n_izd in (select n_izd
                                    from j
                                    where name like '______%')
      EXCEPT
      SELECT DISTINCT s.n_post,s.name,s.town, s.reiting
      FROM spj
      JOIN s ON s.n_post=spj.n_post
      WHERE n_det not IN (SELECT n_det
                          FROM p
                          WHERE cvet='Красный')
                          and n_izd in (select n_izd
                                        from j
                                        where name like '______%')
   if (sqlca.sqlcode < 0) // проверка объявления
   {
      printf("declare error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work;
      return;
   }
   exec sql begin work; //начало транзакци
   exec sql open curs3;   // открываем курсор
   if (sqlca.sqlcode < 0) // проверка открытия
   {
      printf("open error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs3;
      exec sql rollback work;
      return;
   }
   exec sql fetch curs3; // следующая строка из активного множества
   if (sqlca.sqlcode < 0) 
   {
   
      printf("fetch error! %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs3;
      exec sql rollback work; 
      return;
   }
   if (sqlca.sqlcode == 100)
   {
      printf("No results found\n");
      exec sql commit work;
      return;
   }
   int r_count = 1;
   printf("|n_post |name            |reiting         |town         |\n");
   printf("|%.6s|%.20s|%d|%.20s|\n", n_post, name, reiting, town);
   while (sqlca.sqlcode == 0) // Пока не дошли до конца активного множества
   {
      exec sql fetch curs3; // следующая строка из активного множества
      if (sqlca.sqlcode == 0)
      {
         printf("|%.6s|%.20s|%d|%.20s|\n", n_post, name, reiting, town);
         r_count += 1;
      }
   }
   if (sqlca.sqlcode == 100)
   {
      exec sql close curs3; // закрытие курсора
      printf("Success!\n");
      printf("Rows processed: %d\n", r_count);
      exec sql commit work;
      return;
   }
   if (sqlca.sqlcode < 0)
   {
      printf("fetch error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs3;
      exec sql rollback work; 
      return;
   }
}

int main()
{
   ConnectDB();
   while(true)
   {
      printf("What to do?\n");
      PrintMenu();
      printf("Choose the number: ");
      int number = 0;
      scanf("%d", &number);
      switch (number)
      {
         case 1:
            Task1();
            break;
         case 2:
            Task2();
            break;
         case 3:
            Task3();
            break;
         case 4:
            Task4();
            break;
         case 5:
            Task5();
            break;
         case 6:
            DisconnectDB();
            return 0;
         default:
            printf("Try again!\n");
            return 0;
         break;
      }
   }
}
