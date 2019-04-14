c     izamaxsub.f
c
c     The program is a fortran wrapper for izamax.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine izamaxsub(n,x,incx,iamax)
c
      external izamax
      integer  izamax,iamax
      integer n,incx
      double complex x(*)
c
      iamax=izamax(n,x,incx)
      return
      end
