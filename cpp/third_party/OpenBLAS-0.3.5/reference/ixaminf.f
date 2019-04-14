      integer function ixaminf(n,zx,incx)
c
c     finds the index of element having min. absolute value.
c     jack dongarra, 1/15/85.
c     modified 3/93 to return if incx .le. 0.
c     modified 12/3/93, array(1) declarations changed to array(*)
c
      complex*20 zx(*)
      real*10 smin
      integer i,incx,ix,n
      real*10 qcabs1
c
      ixaminf = 0
      if( n.lt.1 .or. incx.le.0 )return
      ixaminf = 1
      if(n.eq.1)return
      if(incx.eq.1)go to 20
c
c        code for increment not equal to 1
c
      ix = 1
      smin = qcabs1(zx(1))
      ix = ix + incx
      do 10 i = 2,n
         if(qcabs1(zx(ix)).ge.smin) go to 5
         ixaminf = i
         smin = qcabs1(zx(ix))
    5    ix = ix + incx
   10 continue
      return
c
c        code for increment equal to 1
c
   20 smin = qcabs1(zx(1))
      do 30 i = 2,n
         if(qcabs1(zx(i)).ge.smin) go to 30
         ixaminf = i
         smin = qcabs1(zx(i))
   30 continue
      return
      end
