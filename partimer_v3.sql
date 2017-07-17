with internal as (
select rgn.id as region_id, rgn.name as region_name, rgn.brand_id, rgn.region_level, rgn.structure_type, rgn.is_blocked, rgn.code as region_code, rgn.status as region_status, rgn.education_region_id,
usr.id as user_id, usr.last_name || ' ' || usr.first_name as full_name,  usr.email, usr.mobile_number, usr.city
from regions as rgn
left join user_post_brands as upb on rgn.id = upb.region_id
left join user_posts as usp on usp.id = upb.user_post_id
left join users as usr on usp.user_id = usr.id),
internal_hrr as (
select inte.region_id, inte.region_name, inte.brand_id, inte.region_level, inte.structure_type, inte.is_blocked, inte.region_code, inte.region_status, inte.education_region_id,
inte.user_id, inte.full_name,  inte.email, inte.mobile_number, inte.city, 
rgh5.ancestor_id as "n1_region_id", l5.user_id as "n1_user_id", l5.full_name as "n1_full_name", 
rgh4.ancestor_id as "n2_region_id", l4.user_id as "n2_user_id", l4.full_name as "n2_full_name", 
rgh3.ancestor_id as "n3_region_id", l3.user_id as "n3_user_id", l3.full_name as "n3_full_name"
from internal as inte
left join region_hierarchies as rgh5 on rgh5.descendant_id = inte.region_id and rgh5.generations = 1
left join internal as l5 on  rgh5.ancestor_id = l5.region_id
left join region_hierarchies as rgh4 on rgh4.descendant_id = inte.region_id and rgh4.generations = 2
left join internal as l4 on  rgh4.ancestor_id = l4.region_id
left join region_hierarchies as rgh3 on rgh3.descendant_id = inte.region_id and rgh3.generations = 3
left join internal as l3 on  rgh3.ancestor_id = l3.region_id
),
region_sr as (
select 
rgn.id, rgn.name as ter_name, brd."name" as brand, rgn.code as ter_code, rgn.status as ter_status, 
rgn1.id as reg_id, rgn1.name as reg_name, rgn1.code as reg_code, rgn1.status as reg_status, rgn1.education_region_id,
rgn2.name as mreg_name, rgn2.code as mreg_code, rgn2.status as mreg_status
from regions as rgn
left join regions as rgn1 on rgn.parent_id = rgn1.id
left join regions as rgn2 on rgn1.parent_id = rgn2.id
left join brands as brd on rgn.brand_id = brd.id
where rgn.region_level = 6 and rgn.structure_type = 1),
salons_rgn as (select sln.id, brand, sln."name" ||'. '|| sln.address || '. ' || sln.city as salon_name,   sln.city,  slt."name" as salon_type, 
rgu.id as com_ter_id, rgu.ter_name as com_ter_name, rgu.reg_id as com_reg_id, rgu.reg_name as com_reg_name, rgu.mreg_name as com_mreg_name, rgu.education_region_id as education_region_id
from  salons as sln 
left join salon_types as slt on sln.salon_type_id = slt.id
left join regions_salons as rgs on sln.id = rgs.salon_id
left join region_sr as rgu on rgs.region_id = rgu.id
order by sln.id)
---
select
brn.pretty_name,
sme.educator_id as educater_id,
to_char(sme.started_at,'DD') as Day, 
to_char(sme.started_at,'MM')as Month,
to_char(sme.started_at,'YYYY') as Year ,
to_char(sme.started_at,'dd.mm.YYYY') as FullDate,
smr.duration,
smr."name",
rgn_edu.name as region,
(case when sme.business_trip is true then 1 else 0 end)  as trip,
(case  when sme.is_closed  then '1' else 0 end) as seminar_closed,
edu.first_name || ' ' || edu.last_name as educator_name,
(case  when sme.participants_count = '0' then 0 else 1 end) as Users_Count,
(case when sme.studio_id is not null then  trc."name" || ' ' || trc.address
	else 'in_salon: ' || sln.salon_name  end) as name_place,
(case when sme.studio_id is not null then 'studio' else 'in_salon' end) as type_place,
(case inte.region_level
	when 6 then 'technolog' 
	when 5 then 'manager'
	when 4 then 'reg_technolog'
	end) as  role_name,
inte.n1_full_name, inte.n2_full_name, inte.n3_full_name,
sln.com_ter_name, sln.com_reg_name, sln.com_mreg_name, sln.salon_type
from seminar_events as sme
left join seminars as smr on sme.seminar_id =smr.id
left join training_centers as trc on sme.studio_id = trc.id
left join brands as brn on smr.brand_id = brn.id
left join salons_rgn as sln on sme.salon_id =sln.id and brn."name" = sln.brand
left join users as edu on sme.educator_id = edu.id
left join seminar_event_types as smret on sme.seminar_event_type_id = smret.id 
left join participations as prt on sme.id = prt.seminar_event_id
left join users as prtu on prt.user_id = prtu.id
left join users as prtru on prt.recorded_user_id = prtru.id
left join regions as rgn_edu on sme.region_id =rgn_edu.id
left join internal_hrr as inte on sme.educator_id = inte.user_id
where to_char(sme.started_at,'YYYY') in ('2017') and brn."name" is not null and sme.region_id is not null --and inte.education_region_id is not null


select 


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
,
to_char(smr.closed_at,'dd.mm.YYYY') as closed_date  

