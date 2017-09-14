with 
educater as (
	select 
		sme.educator_id, count(distinct sme.id) as count
	from seminar_events as sme
	group by sme.educator_id),
---------------------------
users_r as (
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
		usr.mobile_number as mobile_number, to_char(usr.last_request_at,  'DD.MM.YYYY') as last_request,
		usr.state, usr.technolog_salary_category, usr.login,
		(select 
			rgn."name" from regions as rgn 
		left join user_posts as usp on rgn.user_post_id = usp.id  
		where usp.user_id  = usr.id limit 1) as region,
		(select 
			rgn.structure_type 
		from regions as rgn 
		left join user_posts as usp on rgn.user_post_id = usp.id  
		where usp.user_id  = usr.id limit 1) as type,
		brn.code
	from users as usr
		left join users_brands as usrb on usr.id = usrb.user_id
		left join brands as brn on usrb.brand_id = brn.id
	where usr.technolog_salary_category is not null
	)
------------------------
select *
from users_r
