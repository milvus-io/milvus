      integer function isaminf(n,sx,incx)
c
c     finds the index of element having min. absolute value.
c     jack dongarra, linpack, 3/11/78.
c     modified 3/93 to return if incx .le. 0.
c     modified 12/3/93, array(1) declarations changed to array(*)
c
      real sx(*),smin
      integer i,incx,ix,n
c
      isaminf = 0
      if( n.lt.1 .or. incx.le.0 ) return
      isaminf = 1
      if(n.eq.1)return
      if(incx.eq.1)go to 20
c
c        code for increment not equal to 1
c
      ix = 1
      smin = abs(sx(1))
      ix = ix + incx
      do 10 i = 2,n
         if(abs(sx(ix)).ge.smin) go to 5
         isaminf = i
         smin = abs(sx(ix))
    5    ix = ix + incx
   10 continue
      return
c
c        code for increment equal to 1
c
   20 smin = abs(sx(1))
      do 30 i = 2,n
         if(abs(sx(i)).ge.smin) go to 30
         isaminf = i
         smin = abs(sx(i))
   30 continue
      return
      end
