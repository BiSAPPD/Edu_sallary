select  smr.at_studio,
(case when smr.studio_id is not NULL then '1' else 0 end) as id_Studio,

to_char(smr.started_at,'DD') as Day, 
to_char(smr.started_at,'MM')as Month, 
to_char(smr.started_at,'YYYY') as Year ,
to_char(smr.started_at,'dd.mm.YYYY') as FullDate,
to_char(smr.updated_at,'dd.mm.YYYY') as UpdateDate,
(case  when to_char (smr.closed_at, 'YYYY') in ('2017')  then '1' else 0 end) as seminar_closed,
smr.seminar_type_id,smt.name, smr.city_name, smr.studio_name, smr.technolog_full_name, smr.users_count, smr.salons_count, usr.full_name, usr.id, 
usr.role,
usr.position, usr.city_name, 
usr.region_name
, usr.shtatnost, usr.chief, usr.full_name, 
(select distinct usr2.chief from users as USR2 where usr2.full_name=usr.chief and usr2.is_blocked = 'f' limit 1) as nPlus1
, (select sln.com_sect from salons as sln where smr.salon_id = sln.id) as sec 
, (select sln.com_reg from salons as sln where smr.salon_id = sln.id) as reg 



from seminars AS SMR
left join seminar_types as smt on smr.seminar_type_id = smt.id
left join users AS USR ON smr.technolog_id = usr.id or smr.partimer_id = usr.id

where to_char(started_at, 'YYYY') in ('2017')  and to_char(started_at, 'MM') in ('01', '02', '03') 