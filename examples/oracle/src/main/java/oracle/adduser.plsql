create or replace procedure addUser(
 x_firstname IN varchar2,
 x_lastname IN varchar2,
 x_userId OUT number
)
is
begin
 select SI_USERS_S.NEXTVAL into x_userId from sys.dual;
 insert into si_users_t(user_id, first_name, last_name) values(x_userId, x_firstname, x_lastname);
end;
