USE sparkstreaming;

CREATE TABLE estudiantes (
identificador text,
nombre text,
edad int,
matricula time,
PRIMARY KEY ((identificador,matricula),edad));


INSERT INTO estudiantes (identificador, nombre,edad, matricula) 
VALUES ('Isa23', 'Isabel', 23, '13:30:54.234');


