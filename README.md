slick-recursive-insert
======================

test of recursive insert with help of [Slick](http://slick.lightbend.com/)

The project demonstrate the insert of recursive record 
in a table in order to model the persistence of a query
where clause:

```sql
 ... WHERE field1 eq val1 AND field2 lt val2 AND (field3 gt val3 OR field4 eq val4)
```

which is equivalent to:

```scala
and(  and(eq(val1,field1), lt(field2,val2)) , or(gt(field3,val3), eq(field4,val4))  )
```

to be inserted the data must be in order of DFS (Deep First Search) 
considering a tree formed by the `where` clause.

```scala
 Seq(
   (AND,    None  ,   None)
   (AND,    None  ,   None)
   (EQ ,    field1,   val1)
   (LT ,    field2,   val2)
   (OR ,    None  ,   None)
   (GT ,    field4,   val4)
   (EQ ,    field5,   val5)
 )
```

