select 
(case when smr.technolog_id is not null then smr.technolog_id else
    (case when smr.partimer_id is not null then smr.technolog_id else
        (case when smr.partner_id is not null then smr.partner_id end) end) end) as educater_id,
smr.seminar_type_id, 
to_char(started_at,'DD') as Day, 
to_char(started_at,'MM')as Month,
to_char(started_at,'YYYY') as Year ,
to_char(started_at,'dd.mm.YYYY') as FullDate , 
smt.duration, 
smt.Name, 
smr.city_name,

(Case when smr.trip is true or false then
    (case when smr.trip = 't' then 1 else 0 end) 
        else
            (case when smr.business_trip = 't' then '1' else 0 end)
                end) as trip,

(case  when to_char (smr.closed_at, 'YYYY') in ( '2016')  then '1' else 0 end) as seminar_closed,

(case when smr.partimer_id is not null then smr.partimer_full_name else
    (case when smr.technolog_id  is not null then smr.technolog_full_name else 
        (case when smr.partner_id  is not null then smr.partner_full_name else 'Not_found' end) end) end) as educater,

(case  when smr.users_count = '0' then 0 else 1 end) as Users_Count,

(case when smr.salon_id is not null or smr.salon_id not in ( '0' ) then Concat('inSalon_', slnPlace.name,'. ',slnPlace.address) else
    (case when smr.studio_id  is not null or smr.studio_id not in ( '0' ) then concat(std.name, '. ',std.address)  else '' end)
        end) as name_place,

(case when smr.salon_id is not null or smr.salon_id not in ( '0' )then 'SALON' else
    (case when smr.studio_id  is not null and (Select std.studio_type from studios as std where smr.studio_id = std.id) = 'classroom' then 'CLASSROOM' else 
        (case when smr.studio_id  is not null and (Select std.studio_type from studios as std where smr.studio_id = std.id) = 'studio' then 'STUDIO' else  'NOT_TYPE' 
            end) end) end) as type_place,

(case when smr.technolog_id is not null then (select usr_edu.role from users as usr_edu where smr.technolog_id = usr_edu.id) else
    (case when smr.partimer_id is not null then (select usr_edu.role from users as usr_edu where smr.partimer_id = usr_edu.id) else
        (case when smr.partner_id is not null then (select usr_edu.role from users as usr_edu where smr.partner_id = usr_edu.id)
            end) end )end) as edu_role,

(Case when smr.salon_id is not null then slnPlace.com_mreg end) as com_mreg,
(Case when smr.salon_id is not null then slnPlace.com_reg end) as com_reg,
(Case when smr.salon_id is not null then slnPlace.com_sect end) as com_sect,
(Case when smr.salon_id is not null then slnPlace.client_type end) as client_type,

(Case when smr.salon_id is not null then
	(Case when slnPlace.is_closed = 't' then 'in_ptnc_base' else 'in_act_base' end)end) as active_clnt,

(select count(smu.user_id) from seminar_users as smu where smr.id = smu.seminar_id) as count_smr_users,

(select count(Distinct usr.email) 
    from seminar_users as smu
    left join users as usr ON smu.user_id = usr.id
    where smr.id = smu.seminar_id) as count_usr_emails,

(select count( usr.last_request_at) 
    from seminar_users as smu
    left join users as usr ON smu.user_id = usr.id
    where smr.id = smu.seminar_id) as count_ecad_user,
    
(select count(Distinct usr.salon_id) 
    from seminar_users as smu
    left join users as usr ON smu.user_id = usr.id
    where smr.id = smu.seminar_id) as count_usr_salons
  
from seminars as smr
left join seminar_types as smt ON smr.seminar_type_id = smt.id
left join salons as slnPlace ON smr.salon_id is not null and smr.salon_id = slnPlace.id
left join studios as std ON smr.studio_id is not null and smr.studio_id = std.id

where to_char(started_at,'YYYY')in ('2016')