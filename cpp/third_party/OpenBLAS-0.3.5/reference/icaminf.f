      integer function icaminf(n,cx,incx)
c
c     finds the index of element having min. absolute value.
c     jack dongarra, linpack, 3/11/78.
c     modified 3/93 to return if incx .le. 0.
c     modified 12/3/93, array(1) declarations changed to array(*)
c
      complex cx(*)
      real smin
      integer i,incx,ix,n
      real scabs1
c
      icaminf = 0
      if( n.lt.1 .or. incx.le.0 ) return
      icaminf = 1
      if(n.eq.1)return
      if(incx.eq.1)go to 20
c
c        code for increment not equal to 1
c
      ix = 1
      smin = scabs1(cx(1))
      ix = ix + incx
      do 10 i = 2,n
         if(scabs1(cx(ix)).ge.smin) go to 5
         icaminf = i
         smin = scabs1(cx(ix))
    5    ix = ix + incx
   10 continue
      return
c
c        code for increment equal to 1
c
   20 smin = scabs1(cx(1))
      do 30 i = 2,n
         if(scabs1(cx(i)).ge.smin) go to 30
         icaminf = i
         smin = scabs1(cx(i))
   30 continue
      return
      end
