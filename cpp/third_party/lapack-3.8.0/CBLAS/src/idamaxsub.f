c     icamaxsub.f
c
c     The program is a fortran wrapper for idamax.
c     Witten by Keita Teranishi.  2/22/1998
c
      subroutine idamaxsub(n,x,incx,iamax)
c
      external idamax
      integer  idamax,iamax
      integer n,incx
      double precision x(*)
c
      iamax=idamax(n,x,incx)
      return
      end
