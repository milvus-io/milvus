c     dnrm2sub.f
c
c     The program is a fortran wrapper for dnrm2.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine dnrm2sub(n,x,incx,nrm2)
c
      external dnrm2
      double precision dnrm2,nrm2
      integer n,incx
      double precision x(*)
c
      nrm2=dnrm2(n,x,incx)
      return
      end
