drop function bitset_aggregate;
drop function bitset_or;
drop function bitset_and;
drop function bitset_create;
drop function bitset_intersects;

\! cp /home/todd/val_limit_udf/libudf_bitset.so /usr/lib/

create aggregate function  bitset_aggregate returns string soname 'libudf_bitset.so';
create function bitset_or returns string soname 'libudf_bitset.so';
create function bitset_and returns string soname 'libudf_bitset.so';
create function bitset_create returns string soname 'libudf_bitset.so';
create function bitset_intersects returns integer soname 'libudf_bitset.so';

drop temporary table if exists ag_bitsets;
create temporary table ag_bitsets as select album_id, bitset_aggregate(genre_id, 22) bs from AlbumGenre group by album_id;

set @bsa = bitset_create(1,2,3);
set @bsb = bitset_create(3,4,5);
select hex(@bsa), hex(@bsb), hex(bitset_and(@bsa, @bsa));

select hex(bitset_create(1,2,3));

set @bsa := (select bitset_aggregate(id, 22) from Genre where id > 80);
set @bsb := (select bitset_aggregate(id, 22) from Genre where id < 80);

select hex(@bsa), hex(@bsb), hex(bitset_or(@bsa, @bsb))\G
select hex(@bsa), hex(@bsb), hex(bitset_and(@bsa, @bsb))\G
