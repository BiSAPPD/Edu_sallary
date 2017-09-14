---internal выводит структуру регионов сотурудников обучения сom и edu 
with 
internal as (
select 
	rgn.id as region_id, rgn.name as region_name, rgn.brand_id, rgn.region_level, rgn.structure_type, rgn.is_blocked, rgn.code as region_code, rgn.status as region_status, rgn.education_region_id,
	usr.id as user_id, usr.last_name || ' ' || usr.first_name as full_name,  usr.email, usr.mobile_number, usr.city
from regions as rgn
	left join user_post_brands as upb on rgn.id = upb.region_id
	left join user_posts as usp on usp.id = upb.user_post_id
	left join users as usr on usp.user_id = usr.id),
---internal_hrr выводит структру с вышестоящими регионами на три уровня выше
internal_hrr as (
select 
	distinct inte.brand_id, inte.region_level, inte.structure_type, 
	inte.user_id, inte.full_name,  inte.email, inte.mobile_number, inte.city, 
	l5.user_id as "n1_user_id", l5.full_name as "n1_full_name", l5.email as n1_email,
	l4.user_id as "n2_user_id", l4.full_name as "n2_full_name", 
	l3.full_name as "n3_full_name"
from internal as inte
	left join region_hierarchies as rgh5 on rgh5.descendant_id = inte.region_id and rgh5.generations = 1
	left join internal as l5 on  rgh5.ancestor_id = l5.region_id
	left join region_hierarchies as rgh4 on rgh4.descendant_id = inte.region_id and rgh4.generations = 2
	left join internal as l4 on  rgh4.ancestor_id = l4.region_id
	left join region_hierarchies as rgh3 on rgh3.descendant_id = inte.region_id and rgh3.generations = 3
	left join internal as l3 on  rgh3.ancestor_id = l3.region_id)
---
select
usr.last_name || ' ' || usr.first_name as ФИО, usr.id, 
		(case usr.technolog_salary_category
				when 8 then 'Art_Parnter' 
				when 7 then 'Partimer'
				when 6 then 'RTM'
				when 5 then 'Indirect_technolog'
				when 4 then 'Technolog'
				when 3 then 'SAE_FIX'
				when 2 then 'MAE'
				when 1 then 'SAE'
				when 0 then 'AE'
				end) as  Позиция,  
		(case usr.technolog_salary_category
				when 8 then 'Art_Parnter' 
				when 7 then '2000'
				when 6 then '0'
				when 5 then '2200'
				when 4 then 'Technolog'
				when 3 then '0'
				when 2 then '0'
				when 1 then '2200'
				when 0 then '2000'
				end) as  Позиция,  
		(case usr.state
				when 1 then 'Loreal'
				when 0 then 'Ankor'
				end) as  Штатность,
		usr.city as Город_базирования, 
		usr.email as mail, 
		usr.mobile_number as mobile_number, 
		to_char(usr.last_request_at,  'DD.MM.YYYY') as last_request,
		usr.login,
		brn.code as brand,
		inteh.n1_full_name, inteh.n1_email, inteh.n2_full_name, inteh.n3_full_name
from users as usr
	left join internal as inte on usr.id = inte.user_id
	left join internal_hrr as inteh on usr.id = inteh.user_id
 	left join brands as brn on inteh.brand_id = brn.id
 where usr.technolog_salary_category is not null