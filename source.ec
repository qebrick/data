#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

/* Îáúÿâëåíèå ïåðåìåííûõ äëÿ ïîäêëþ÷åíèÿ ê ÁÄ */
exec SQL begin declare section;
    char db_name[50];      /* Èìÿ áàçû äàííûõ */
    char user[50];         /* Ëîãèí */
    char password[50];     /* Ïàðîëü */
exec SQL end declare section;

void ConnectDB() 
{

      strcpy(db_name, "students"); // Èìÿ áàçû äàííûõ
      strcpy(user, "pmi-b1408"); // Ëîãèí
      strcpy(password, "Lokdiew4$"); // Ïàðîëü
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
   1. Âûäàòü ÷èñëî èçäåëèé, äëÿ êîòîðûõ äåòàëè ñ âåñîì áîëüøå 12
      ïîñòàâëÿë ïåðâûé ïî àëôàâèòó ïîñòàâùèê.
   */
   exec sql begin declare section;
      int count; // Ðåçóëüòàò çàïðîñà - ÷èñëî èçäåëèé
   exec sql end declare section;
   printf("Starting Task1 request processing...\n");
   exec sql begin work; //íà÷àëî òðàíçàêöèè
   exec sql select count(distinct spj.n_izd)
            from spj
            inner join s on s.n_post=spj.n_post
            inner join p on p.n_det=spj.n_det
            where s.name=(select min(name) from s) and p.ves>12
   if (sqlca.sqlcode < 0) //ïðîâåðêà êîäà âîçâðàòà çàïðîñà
   {
      printf("Task1 error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work; // îòìåíà âñåõ èçìåíåíèé â ðàìêàõ òðàíçàêöèè
      return;
   }
   else // åñëè óñïåøíî çàâåðøåíî
   {
      printf("Success! code %d\n", sqlca.sqlcode);
      printf("Count: %d\n", count);
      exec sql commit work; // êîíåö òðàíçàêöèè
      return;
   }
}

void Task2()
{
   /*
   2. Ïîìåíÿòü ìåñòàìè ôàìèëèè ïåðâîãî è ïîñëåäíåãî ïî àëôàâèòó
      ïîñòàâùèêà, ò. å. ïåðâîìó ïî àëôàâèòó ïîñòàâùèêó óñòàíîâèòü ôàìè-
      ëèþ ïîñëåäíåãî ïî àëôàâèòó ïîñòàâùèêà è íàîáîðîò.
   */
   printf("Starting Task2 request processing...\n");
   exec sql begin work; //íà÷àëî òðàíçàêöè
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
   if (sqlca.sqlcode == 100) //ïðîâåðêà íà îòñóòñòâèå äàííûõ
   {
      printf("There is no data to update!\n");
      return;
   }   
   if (sqlca.sqlcode == 0)
   {
      printf("Success! code %d\n", sqlca.sqlcode);
      printf("Changes made: %d\n", sqlca.sqlerrd[2]);
      exec sql commit work; // êîíåö òðàíçàêöèè
      return;
   }
}


void Task3()
{
   /*
   3. Íàéòè èçäåëèÿ, äëÿ êîòîðûõ âûïîëíåíû ïîñòàâêè, âåñ êîòîðûõ
      áîëåå ÷åì â 4 ðàçà ïðåâûøàåò ìèíèìàëüíûé âåñ ïîñòàâêè äëÿ èçäåëèÿ.
      Âûâåñòè íîìåð èçäåëèÿ, âåñ ïîñòàâêè, ìèíèìàëüíûé âåñ ïîñòàâêè äëÿ
      èçäåëèÿ.
   */
   exec sql begin declare section;
      char n_izd[6]; // Ðåçóëüòàò çàïðîñà - íîìåðà äåòàëåé
   exec sql end declare section;
   printf("Starting Task3 request processing...\n");
   // îáúÿâëåíèå êóðñîðà
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
   if (sqlca.sqlcode < 0) // ïðîâåðêà îáúÿâëåíèÿ
   {
      printf("declare error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work;
      return;
   }
   exec sql begin work; //íà÷àëî òðàíçàêöè
   exec sql open curs1;   // îòêðûâàåì êóðñîð
   if (sqlca.sqlcode < 0) // ïðîâåðêà îòêðûòèÿ
   {
      printf("open error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs1;
      exec sql rollback work;
      return;
   }
   exec sql fetch curs1; // ñëåäóþùàÿ ñòðîêà èç àêòèâíîãî ìíîæåñòâà
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
   while (sqlca.sqlcode == 0) // Ïîêà íå äîøëè äî êîíöà àêòèâíîãî ìíîæåñòâà
   {
      exec sql fetch curs1; // ñëåäóþùàÿ ñòðîêà èç àêòèâíîãî ìíîæåñòâà
      if (sqlca.sqlcode == 0)
      {
         printf("%s\n", n_izd);
         r_count += 1;
      }
   }
   if (sqlca.sqlcode == 100)
   {
      exec sql close curs1; // çàêðûòèå êóðñîðà
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
   4. Âûáðàòü ïîñòàâùèêîâ, íå ïîñòàâèâøèõ íè îäíîé èç äåòàëåé,
      èìåþùèõ íàèìåíüøèé âåñ.
   */
   exec sql begin declare section;
      char n_post[6]; // Ðåçóëüòàò çàïðîñà - íîìåðà ïîñòàâùèêîâ
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
   if (sqlca.sqlcode < 0) // ïðîâåðêà îáúÿâëåíèÿ
   {
      printf("declare error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work;
      return;
   }
   exec sql begin work; //íà÷àëî òðàíçàêöè
   exec sql open curs2;   // îòêðûâàåì êóðñîð
   if (sqlca.sqlcode < 0) // ïðîâåðêà îòêðûòèÿ
   {
      printf("open error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs2;
      exec sql rollback work;
      return;
   }
   exec sql fetch curs2; // ñëåäóþùàÿ ñòðîêà èç àêòèâíîãî ìíîæåñòâà
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
   while (sqlca.sqlcode == 0) // Ïîêà íå äîøëè äî êîíöà àêòèâíîãî ìíîæåñòâà
   {
      exec sql fetch curs2; // ñëåäóþùàÿ ñòðîêà èç àêòèâíîãî ìíîæåñòâà
      if (sqlca.sqlcode == 0)
      {
         printf("%s\n", n_post);
         r_count += 1;
      }
   }
   if (sqlca.sqlcode == 100)
   {
      exec sql close curs2; // çàêðûòèå êóðñîðà
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
   5. Âûäàòü ïîëíóþ èíôîðìàöèþ î ïîñòàâùèêàõ, ïîñòàâëÿþùèõ
   ÒÎËÜÊÎ êðàñíûå äåòàëè è òîëüêî äëÿ èçäåëèÿ ñ äëèíîé íàçâàíèÿ íå
   ìåíüøå 7
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
                      WHERE cvet='Êðàñíûé') 
                      and n_izd in (select n_izd
                                    from j
                                    where name like '______%')
      EXCEPT
      SELECT DISTINCT s.n_post,s.name,s.town, s.reiting
      FROM spj
      JOIN s ON s.n_post=spj.n_post
      WHERE n_det not IN (SELECT n_det
                          FROM p
                          WHERE cvet='Êðàñíûé')
                          and n_izd in (select n_izd
                                        from j
                                        where name like '______%')
   if (sqlca.sqlcode < 0) // ïðîâåðêà îáúÿâëåíèÿ
   {
      printf("declare error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work;
      return;
   }
   exec sql begin work; //íà÷àëî òðàíçàêöè
   exec sql open curs3;   // îòêðûâàåì êóðñîð
   if (sqlca.sqlcode < 0) // ïðîâåðêà îòêðûòèÿ
   {
      printf("open error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs3;
      exec sql rollback work;
      return;
   }
   exec sql fetch curs3; // ñëåäóþùàÿ ñòðîêà èç àêòèâíîãî ìíîæåñòâà
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
   while (sqlca.sqlcode == 0) // Ïîêà íå äîøëè äî êîíöà àêòèâíîãî ìíîæåñòâà
   {
      exec sql fetch curs3; // ñëåäóþùàÿ ñòðîêà èç àêòèâíîãî ìíîæåñòâà
      if (sqlca.sqlcode == 0)
      {
         printf("|%.6s|%.20s|%d|%.20s|\n", n_post, name, reiting, town);
         r_count += 1;
      }
   }
   if (sqlca.sqlcode == 100)
   {
      exec sql close curs3; // çàêðûòèå êóðñîðà
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
