# voxtecnologiahub-dbinterview
voxtecnologia\ test
Realizado um estudo de base, sendo iniciado o processo de localização e acesso dos dados via PostgreSQL docker atraves do powershell.
Localizado na base de dados as tabela e dados da mesmo segue as tabela localizada na database:dbinterview 

tabelas disponíveis:
SELECT tablename FROM pg_tables WHERE schemaname = 'public';
   tablename
---------------
 actor
 store
 address
 category
 city
 country
 customer
 film_actor
 film_category
 inventory
 language
 rental
 staff
 payment
 film
 
-Apos o estudo das tabelas foram identificados os seguintes relacionamentos:

Relacionamento entre tabelas:

Tabela actor
 Relacionamento:
 TABLE "film_actor" CONSTRAINT "film_actor_actor_id_fkey" FOREIGN KEY (actor_id) REFERENCES actor(actor_id)
 
Tabela address
 Relacionamento:
  TABLE "customer" CONSTRAINT "customer_address_id_fkey" FOREIGN KEY (address_id) REFERENCES address(address_id) 
    TABLE "staff" CONSTRAINT "staff_address_id_fkey" FOREIGN KEY (address_id) REFERENCES address(address_id) 
    TABLE "store" CONSTRAINT "store_address_id_fkey" FOREIGN KEY (address_id) REFERENCES address(address_id)

Tablea category
 Relacionamento:
 TABLE "film_category" CONSTRAINT "film_category_category_id_fkey" FOREIGN KEY (category_id) REFERENCES category(category_id) 

Tabela city:
Relacionamento
Foreign-key constraints:
    "fk_city" FOREIGN KEY (country_id) REFERENCES country(country_id)
Referenced by:
    TABLE "address" CONSTRAINT "fk_address_city" FOREIGN KEY (city_id) REFERENCES city(city_id)
	
Tabela country
 Relacionamento:
    TABLE "city" CONSTRAINT "fk_city" FOREIGN KEY (country_id) REFERENCES country(country_id)
	
Tabela customer
 Relacionamento:
	Foreign-key constraints:
    "customer_address_id_fkey" FOREIGN KEY (address_id) REFERENCES address(address_id) 
 Referenced by:
    TABLE "payment" CONSTRAINT "payment_customer_id_fkey" FOREIGN KEY (customer_id) REFERENCES customer(customer_id) 
    TABLE "rental" CONSTRAINT "rental_customer_id_fkey" FOREIGN KEY (customer_id) REFERENCES customer(customer_id) 
	
Tabela film
 Relacionamento:
	Foreign-key constraints:
    "film_language_id_fkey" FOREIGN KEY (language_id) REFERENCES language(language_id) 
 Referenced by:
    TABLE "film_actor" CONSTRAINT "film_actor_film_id_fkey" FOREIGN KEY (film_id) REFERENCES film(film_id) 
    TABLE "film_category" CONSTRAINT "film_category_film_id_fkey" FOREIGN KEY (film_id) REFERENCES film(film_id)
    TABLE "inventory" CONSTRAINT "inventory_film_id_fkey" FOREIGN KEY (film_id) REFERENCES film(film_id) 

Tabela film_actor:
 Relacionamento:
  Foreign-key constraints:
    "film_actor_actor_id_fkey" FOREIGN KEY (actor_id) REFERENCES actor(actor_id) 
    "film_actor_film_id_fkey" FOREIGN KEY (film_id) REFERENCES film(film_id) 

Tabela film_category:
 Relacionamento:
	Foreign-key constraints:
    "film_category_category_id_fkey" FOREIGN KEY (category_id) REFERENCES category(category_id) 
	ilm_category_film_id_fkey" FOREIGN KEY (film_id) REFERENCES film(film_id) 

Tabela language
 Relacionamento:
	TABLE "film" CONSTRAINT "film_language_id_fkey" FOREIGN KEY (language_id) REFERENCES language(language_id) 
	
Tabela payment
 Relacionamento:
	Foreign-key constraints:
    "payment_customer_id_fkey" FOREIGN KEY (customer_id) REFERENCES customer(customer_id) 
    "payment_rental_id_fkey" FOREIGN KEY (rental_id) REFERENCES rental(rental_id) 
    "payment_staff_id_fkey" FOREIGN KEY (staff_id) REFERENCES staff(staff_id) 
	
Tabela rental
 Relacionamento:
	Foreign-key constraints:
    "rental_customer_id_fkey" FOREIGN KEY (customer_id) REFERENCES customer(customer_id) 
    "rental_inventory_id_fkey" FOREIGN KEY (inventory_id) REFERENCES inventory(inventory_id) 
    "rental_staff_id_key" FOREIGN KEY (staff_id) REFERENCES staff(staff_id)
	Referenced by:
    TABLE "payment" CONSTRAINT "payment_rental_id_fkey" FOREIGN KEY (rental_id) REFERENCES rental(rental_id) 
	
Tabela staff
	Relacionamento:
		Foreign-key constraints:
		"staff_address_id_fkey" FOREIGN KEY (address_id) REFERENCES address(address_id) 
		Referenced by:
		TABLE "payment" CONSTRAINT "payment_staff_id_fkey" FOREIGN KEY (staff_id) REFERENCES staff(staff_id) 
		TABLE "rental" CONSTRAINT "rental_staff_id_key" FOREIGN KEY (staff_id) REFERENCES staff(staff_id)
		TABLE "store" CONSTRAINT "store_manager_staff_id_fkey" FOREIGN KEY (manager_staff_id) REFERENCES staff(staff_id) 

Tabela store
 Relacionamento:
	Foreign-key constraints:
    "store_address_id_fkey" FOREIGN KEY (address_id) REFERENCES address(address_id) 
    "store_manager_staff_id_fkey" FOREIGN KEY (manager_staff_id) REFERENCES staff(staff_id) 

-Foi solicitado o processo de construção das tabela para a construção do data warehouse sendo gerado o codigo a baixo identificado como Mart para etl.

Construa as seguintes tabelas derivadas (marts):

construção SQL para tabela:

`mart_customer_lifetime_value`: valor total gasto por cliente e tempo desde a primeira locação.

select a.customer_id,b.first_name,b.last_name,b.amount,a.tempo_loc
	   from (select customer_id,(max(return_date)-min(rental_date))as tempo_loc  from rental group by customer_id)a
 inner join(select payment.customer_id,customer.first_name,customer.last_name,(sum (payment.amount))as amount from payment
   inner join customer on payment.customer_id=customer.customer_id group by  payment.customer_id,customer.first_name,customer.last_name)b
 on a.customer_id=b.customer_id
order by a.customer_id;
 
 mart_film_popularity`: ranking de filmes mais alugados por período (mês/ano).
 
 select film_id,title,(count(film_id)) as qnt_loc,rental_date from (select inventory.film_id,film.title,(to_char(rental.rental_date,'MM/YYYY') )as rental_date from rental 
inner join inventory on rental.inventory_id=inventory.inventory_id inner join film on inventory.film_id=film.film_id )a
 group by film_id,rental_date,title order by qnt_loc desc, rental_date desc
 
 mart_store_performance: 

select rental.staff_id, (count(rental.rental_id))as qnt_loc, (sum(payment.amount))as amount  
from rental inner join payment on rental.rental_id=payment.rental_id group by rental.staff_id;

Foi solicitado mais analise atraves do SQL: 

SQL Analítico:

Quais são os 5 clientes que mais geraram receita no último ano?
select customer_id,(sum(amount)) as amount, staff_id from payment group by customer_id,staff_id order by amount desc  limit 5;

Qual a média de dias entre o aluguel e a devolução por categoria de filme?
select category.name,to_char((max(return_date)-min(rental_date)),'dd')as tempo_loc from rental 
inner join inventory on rental.inventory_id=inventory.inventory_id 
inner join film_category on inventory.film_id=film_category.film_id
inner join category on film_category.category_id=category.category_id
group by category.name;

Quais as 3 cidades com maior volume de locações?
select city.city, (count(rental_id)) as qnt_loc from rental 
inner join customer on  rental.customer_id=customer.customer_id
inner join address on customer.address_id=address.address_id
inner join city on address.city_id=city.city_id
group by city.city
order by qnt_loc desc limit 3;

Gere uma visão agregada de receita mensal para os últimos 24 meses com dados.
select staff_id,(sum(amount))as amount, (to_char(payment_date,'mm/yyyy'))as date from payment group by staff_id, date order by date desc,staff_id limit 24;
