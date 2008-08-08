 drop function bitset_aggregate;
 drop function bitset_or;
 drop function bitset_and;

 create aggregate function  bitset_aggregate returns string soname 'libudf_bitset.so';
 create function bitset_or returns string soname 'libudf_bitset.so';
 create function bitset_and returns string soname 'libudf_bitset.so';

set @bsa := (select bitset_aggregate(id, 22) from Genre where id > 80);
set @bsb := (select bitset_aggregate(id, 22) from Genre where id < 80);

select hex(@bsa), hex(@bsb), hex(bitset_or(@bsa, @bsb))\G
select hex(@bsa), hex(@bsb), hex(bitset_and(@bsa, @bsb))\G