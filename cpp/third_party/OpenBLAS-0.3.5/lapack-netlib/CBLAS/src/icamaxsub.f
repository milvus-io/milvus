c     icamaxsub.f
c
c     The program is a fortran wrapper for icamax.
c     Witten by Keita Teranishi.  2/11/1998
c
      subroutine icamaxsub(n,x,incx,iamax)
c
      external icamax
      integer  icamax,iamax
      integer n,incx
      complex x(*)
c
      iamax=icamax(n,x,incx)
      return
      end
