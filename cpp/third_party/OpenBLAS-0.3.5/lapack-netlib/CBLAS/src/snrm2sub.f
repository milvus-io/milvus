c     snrm2sub.f
c
c     The program is a fortran wrapper for snrm2.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine snrm2sub(n,x,incx,nrm2)
c
      external snrm2
      real snrm2,nrm2
      integer n,incx
      real x(*)
c
      nrm2=snrm2(n,x,incx)
      return
      end
