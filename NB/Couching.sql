with a as (select admin_coach_events.id as id, users.id as coach_id, 
admin_coach_seminars."name" as sem_name, 
admin_coach_events.started_date,
users.first_name || ' ' || users.last_name as coach_last_name, 
admin_coach_events.educator_id
from admin_coach_events
left join admin_coach_seminars
on admin_coach_events.admin_coach_seminar_id = admin_coach_seminars.id
left join users
on admin_coach_events.user_id = users.id)
select a.*, 
users.first_name || ' ' || users.last_name as stolbew, users.last_name as educator_last_name
from a
left join users
on a.educator_id = users.id
