      integer function iqminf(n,dx,incx)
c
c     finds the index of element having min. value.
c     jack dongarra, linpack, 3/11/78.
c     modified 3/93 to return if incx .le. 0.
c     modified 12/3/93, array(1) declarations changed to array(*)
c
      real*10 dx(*),dmin
      integer i,incx,ix,n
c
      iqminf = 0
      if( n.lt.1 .or. incx.le.0 ) return
      iqminf = 1
      if(n.eq.1)return
      if(incx.eq.1)go to 20
c
c        code for increment not equal to 1
c
      ix = 1
      dmin = dx(1)
      ix = ix + incx
      do 10 i = 2,n
         if(dx(ix).ge.dmin) go to 5
         iqminf = i
         dmin = dx(ix)
    5    ix = ix + incx
   10 continue
      return
c
c        code for increment equal to 1
c
   20 dmin = dx(1)
      do 30 i = 2,n
         if(dx(i).ge.dmin) go to 30
         iqminf = i
         dmin = dx(i)
   30 continue
      return
      end
