create table files (
  id          serial primary key,

  right1_id   integer,
  contents    bytea
);

create table relations (
  id          serial primary key,

  name        text not null,

  source      integer references files (id) not null,
  destination integer references files (id) not null,

  unique (name, source)
);

create table rights (
  id          serial primary key,

  group_id    integer not null,
  file_id     integer references files (id) not null,

  next_id     integer references rights (id),

  can_read    boolean not null default false,
  can_write   boolean not null default false,
  can_execute boolean not null default false
);

alter table files
  add constraint right1_id_fkey
    foreign key (right1_id)
    references rights (id);

insert into files (id)
  values (0);
