create table if not exists item (
    item_id integer not null primary key,
    item_type text not null,
    item_title text not null,
    item_url text not null,
    band_id integer not null,
    band_name text not null,
    token text,
    also_collected_count integer not null,
    last_updated integer not null
) strict;

create index if not exists item_last_updated on item(last_updated);

create table if not exists collector (
    fan_id integer not null primary key,
    username text not null unique,
    name text not null,
    token text,
    last_updated integer not null
) strict;

create unique index if not exists collector_username on collector(username);
create index if not exists collector_last_updated on collector(last_updated);

create table if not exists collected_by (
    item_id integer not null references item on delete cascade,
    fan_id integer not null references collector on delete cascade,
    primary key (item_id, fan_id)
) strict;

-- they are separate bc presence in the collects table assumes that all earlier collections are known
create table if not exists collects (
    fan_id integer not null references collector on delete cascade,
    item_id integer not null references item on delete cascade,
    primary key (fan_id, item_id)
) strict;

create table if not exists item_collected_by_queue (
    item_id integer not null primary key references item on delete cascade
) strict;

create table if not exists collector_collection_queue (
    fan_id integer not null primary key references collector on delete cascade
) strict;

create table if not exists collection_target (
    fan_id integer not null primary key references collector on delete cascade,
    stage integer not null,
    count_left integer not null, -- technically redundant, but not that cheap to compute
    count_total integer not null,
    eta integer not null -- technically redundant
) strict;
