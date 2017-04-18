select 
(case when  (row_number() over (Partition by smr.id))  = '1' then smr.id Else Null end) as smr_id, 
(case when  (row_number() over (Partition by smr.id))  = '1' then smr.seminar_type_id end) as smr_type_id, 
(case when  (row_number() over (Partition by smr.id))  = '1' then smt.name end) as smr_name,
(case when  (row_number() over (Partition by smr.id))  = '1' then smr.trip end) as smr_status_trip,
(case when  (row_number() over (Partition by smr.id))  = '1' then smt.kpis_type end) as smr_kpi_type, 
(case when  (row_number() over (Partition by smr.id))  = '1' then smt.duration Else Null end) as smr_duration,

(case when  (row_number() over (Partition by smr.id))  = '1' then
	(select slnR.com_mreg
	from seminar_users as smu1
	left join users as usr1 ON smu1.user_id  = usr1.id 
	left join salons as slnR ON usr1.salon_id = slnR.id
	where smr.id = smu1.seminar_id
	GROUP BY smu1.seminar_id, slnr.com_mreg
	order by count(slnR.com_mreg) desc
	limit 1)
end) as smr_mreg,

(case when  (row_number() over (Partition by smr.id))  = '1' then
	(select slnR.com_reg
	from seminar_users as smu1
	left join users as usr1 ON smu1.user_id  = usr1.id 
	left join salons as slnR ON usr1.salon_id = slnR.id
	where smr.id = smu1.seminar_id
	GROUP BY smu1.seminar_id, slnr.com_reg
	order by count(slnR.com_reg) desc
	limit 1)
end) as smr_reg,

(case when  (row_number() over (Partition by smr.id))  = '1' then
	(select slnR.com_sect
	from seminar_users as smu1
	left join users as usr1 ON smu1.user_id  = usr1.id 
	left join salons as slnR ON usr1.salon_id = slnR.id
	where smr.id = smu1.seminar_id
	GROUP BY smu1.seminar_id, slnr.com_sect
	order by count(slnR.com_sect) desc
	limit 1)
end) as smr_sect,

(case when  (row_number() over (Partition by smr.id))  = '1' then
	(case when smr.name like '%CRAFT%' or  smr.name like '%твор%'  or smr.name like '%МП%' then '1' else 0 end)
 end) as is_craft,

(case when  (row_number() over (Partition by smr.id))  = '1' then
	(case when smr.seminar_type_id in ( '20', '88', '21') then '1' else 0 end)
 end) as is_Day_MX, 

(case when (row_number() over (Partition by smr.id))  = '1' then 
    (case when smt.is_free is true then 'free' else 'paid' end) 
end)  as smr_type,

(case when  (row_number() over (Partition by smr.id))  = '1' then
	sum((Case when std.coefficient is not null then std.coefficient * smt.base_price * (Case when spp.status is not null then 0.5 else 1 end)
		else smt.base_price * (Case when spp.status is not null then 0.5 else 1 end)
end)) over (partition by smr.id) Else Null end) as smr_paid,

(case when row_number() over (partition by smr.id) = 1 then 
    count(usr.id) over (partition by smr.id) 
end) as users_count, 

(case when row_number() over (partition by smr.id) = 1 then
	count(sln.id) over (partition by smr.id) 
end) as clients_count,

(case when  (row_number() over (Partition by smr.id))  = '1' then
	(case when smr.salon_id is not null or smr.salon_id not in ( '0' )then 'SALON' else
		(case when smr.studio_id  is not null and (Select std.studio_type from studios as std where smr.studio_id = std.id) = 'classroom' then 'CLASSROOM' else 
			(case when smr.studio_id  is not null and (Select std.studio_type from studios as std where smr.studio_id = std.id) = 'studio' then 'STUDIO' else  'NOT_TYPE' 
				end) end) end) 
end)as place_type,

(case when  (row_number() over (Partition by smr.id))  = '1' then
	(case when smr.salon_id is not null or smr.salon_id not in ( '0' )then Concat('inSalon_', slnPlace.name,'. ',slnPlace.address) else
	(case when smr.studio_id  is not null or smr.studio_id not in ( '0' ) then concat(std.name, '. ',std.address)  else '' end)
	 end) 
 END) as place_name,

 
(case when  (row_number() over (Partition by smr.id))  = '1' then
	(case when smr.partimer_id is not null then smr.partimer_id else
		(case when smr.technolog_id  is not null then smr.technolog_id else
			(case when smr.partner_id  is not null then smr.partner_id end) end) end)
end) educater_id,

(case when  (row_number() over (Partition by smr.id))  = '1' then
	(case when smr.partimer_id is not null then smr.partimer_full_name else
		(case when smr.technolog_id  is not null then smr.technolog_full_name else 
			(case when smr.partner_id  is not null then smr.partner_full_name else 'Not_found' end) end) end) 
end) as educater_name,

(case when  (row_number() over (Partition by smr.id))  = '1' then
	(case when smr.technolog_id is not null then (select usr_edu.role from users as usr_edu where smr.technolog_id = usr_edu.id) else
		(case when smr.partimer_id is not null then (select usr_edu.role from users as usr_edu where smr.partimer_id = usr_edu.id) else
			(case when smr.partner_id is not null then (select usr_edu.role from users as usr_edu where smr.partner_id = usr_edu.id) 
				end) end )end) 
end) as edu_role,  

(case when  (row_number() over (Partition by smr.id))  = '1' then extract(day from smr.started_at) end) as Day, 
(case when  (row_number() over (Partition by smr.id))  = '1' then extract(month from smr.started_at) end)as Month, 
(case when  (row_number() over (Partition by smr.id))  = '1' then extract(year from smr.started_at) end) as Year,
(case when  (row_number() over (Partition by smr.id))  = '1' then to_char(smr.created_at,'dd.mm.YYYY') end) as smr_createdDate,
(case when  (row_number() over (Partition by smr.id))  = '1' then to_char(smr.started_at,'dd.mm.YYYY') end) as smr_startDate ,
(case when  (row_number() over (Partition by smr.id))  = '1' then to_char(smr.closed_at,'dd.mm.YYYY') end) as smr_closedDate ,
(case when  (row_number() over (Partition by smr.id))  = '1' then (case  when  smr.closed_at is not Null then '1' else 0 end) end) as seminar_closed,

count(usr.id) over (partition by smr.id order by usr.id) as user_num,
smu.user_id,
usr.full_name,
usr.role, 
(case when usr.email is not null then 1 else 0 end ) as status_email,
(case when usr.mobile_number is not null then 1 else 0 end ) as status_mobile,
(case when usr.last_request_at is not null then 1 else 0 end ) as status_ecad_active_user,
(Case when usr.salon_id is not null then 'salon_master' else 
(Case when slnMNG.id is not null then 'salon_master' else 
(Case when usr.id is not null then 'hairdresser' else 'not_reg_user' end) end) end) as type_master,
(Case when std.coefficient is not null then std.coefficient * smt.base_price * (Case when spp.status is not null then 0.5 else 1 end)
    else smt.base_price * (Case when spp.status is not null then 0.5 else 1 end)
     end) as user_must_pay,
pmt.ykassa as usr_used_ykassa,


(Case when usr.salon_id is not null then usr.salon_id else slnMNG.id end) as salon_id,
(Case when usr.salon_id is not null then concat(sln.id, '_', sln.name, '. ',sln.address) else 
(Case when slnMNG.id is not null then concat(slnMNG.id, '_', slnMNG.name, '. ',slnMNG.address) else '' end) end) as salon,
(Case when usr.salon_id is not null then sln.com_mreg else slnMNG.com_mreg end) as com_mreg,
(Case when usr.salon_id is not null then sln.com_reg else slnMNG.com_reg end) as com_reg,
(Case when usr.salon_id is not null then sln.com_sect else slnMNG.com_sect end) as com_sect,
(Case when usr.salon_id is not null then sln.client_type else slnMNG.client_type end) as client_type




from seminars as smr
left join seminar_users as SMU ON smr.id = smu.seminar_id
left join seminar_types as smt on smr.seminar_type_id = smt.id
left join users as usr ON smu.user_id = usr.id
left join salons as sln ON usr.salon_id is not null and usr.salon_id = sln.id
left join salons as slnPlace ON smr.salon_id is not null and smr.salon_id = slnPlace.id
left join salons as slnMNG ON usr.salon_id is null and usr.id = slnMNG.salon_manager_id
left join studios as std ON smr.studio_id is not null and smr.studio_id = std.id

left join 
	dblink('dbname=academie user=readonly password=sdfm6234vsj', 
	'select spcr.status as status, spc.id as id, spc.name as name, spc.brand_id as brand_id, spcr.salon_id as salon_id

	from special_program_club_records as spcr
	left join special_program_clubs as spc ON spcr.club_id = spc.id') AS spp (status  text, id integer, name text, brand_id  integer, salon_id  integer )
	ON
	(Case when usr.salon_id is not null then usr.salon_id else slnMNG.id end) = spp.salon_id and spp.brand_id =  

(Case current_database()
                When 'loreal' then 1
                When 'matrix' then 5
                When 'luxe' then 6
                When 'redken' then 7
                When 'essie' then 3
                End)
	and 
		(case  when spp.name like '%Expert%' then spp.status
			    when spp.name like '%МБК%' then   spp.status
				end)  in ('accepted', 'invited' )

left join 
	 
dblink('dbname=academie user=readonly password=sdfm6234vsj',
	'select distinct brand_id as brand_id, master_id as master_id, seminar_id as seminar_id, (Case when price is not null then 1 end) as ykassa
	from payments') AS pmt (brand_id integer, master_id integer, seminar_id integer, ykassa integer)
 ON  smr.id = pmt.seminar_id and usr.id = pmt.master_id and pmt.brand_id = 

 (Case current_database()
                When 'loreal' then 1
                When 'matrix' then 5
                When 'luxe' then 6
                When 'redken' then 7
                When 'essie' then 3
                End)


where smr.started_at >= '2017-02-01' and smr.started_at < '2017-03-01'


--GROUP BY smr.id,smt.kpis_type, smt.name, slnplace.name, slnplace.address, std.name, std.address , smu.user_id, usr.full_name
Order by smr.id

--limit 1000