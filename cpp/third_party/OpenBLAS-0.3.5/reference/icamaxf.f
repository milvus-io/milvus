      integer function icamaxf(n,cx,incx)
c
c     finds the index of element having max. absolute value.
c     jack dongarra, linpack, 3/11/78.
c     modified 3/93 to return if incx .le. 0.
c     modified 12/3/93, array(1) declarations changed to array(*)
c
      complex cx(*)
      real smax
      integer i,incx,ix,n
      real scabs1
c
      icamaxf = 0
      if( n.lt.1 .or. incx.le.0 ) return
      icamaxf = 1
      if(n.eq.1)return
      if(incx.eq.1)go to 20
c
c        code for increment not equal to 1
c
      ix = 1
      smax = scabs1(cx(1))
      ix = ix + incx
      do 10 i = 2,n
         if(scabs1(cx(ix)).le.smax) go to 5
         icamaxf = i
         smax = scabs1(cx(ix))
    5    ix = ix + incx
   10 continue
      return
c
c        code for increment equal to 1
c
   20 smax = scabs1(cx(1))
      do 30 i = 2,n
         if(scabs1(cx(i)).le.smax) go to 30
         icamaxf = i
         smax = scabs1(cx(i))
   30 continue
      return
      end
