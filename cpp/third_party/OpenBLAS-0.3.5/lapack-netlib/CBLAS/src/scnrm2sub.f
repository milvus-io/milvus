c     scnrm2sub.f
c
c     The program is a fortran wrapper for scnrm2.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine scnrm2sub(n,x,incx,nrm2)
c
      external scnrm2
      real scnrm2,nrm2
      integer n,incx
      complex x(*)
c
      nrm2=scnrm2(n,x,incx)
      return
      end
