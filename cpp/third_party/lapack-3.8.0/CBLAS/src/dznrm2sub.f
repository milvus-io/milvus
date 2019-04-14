c     dznrm2sub.f
c
c     The program is a fortran wrapper for dznrm2.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine dznrm2sub(n,x,incx,nrm2)
c
      external dznrm2
      double precision dznrm2,nrm2
      integer n,incx
      double complex x(*)
c
      nrm2=dznrm2(n,x,incx)
      return
      end
