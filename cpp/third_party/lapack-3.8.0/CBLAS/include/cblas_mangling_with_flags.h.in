#ifndef F77_HEADER_INCLUDED
#define F77_HEADER_INCLUDED

#ifndef F77_GLOBAL
#if defined(F77_GLOBAL_PATTERN_LC) || defined(ADD_)
#define F77_GLOBAL(lcname,UCNAME)  lcname##_
#elif defined(F77_GLOBAL_PATTERN_UC) || defined(UPPER)
#define F77_GLOBAL(lcname,UCNAME)  UCNAME
#elif defined(F77_GLOBAL_PATTERN_MC) || defined(NOCHANGE)
#define F77_GLOBAL(lcname,UCNAME)  lcname
#else
#define F77_GLOBAL(lcname,UCNAME)  lcname##_
#endif
#endif

#endif

