/*****************************************************************************
Copyright (c) 2011-2016, The OpenBLAS Project
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   1. Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
   3. Neither the name of the OpenBLAS project nor the names of 
      its contributors may be used to endorse or promote products 
      derived from this software without specific prior written 
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

**********************************************************************************/

#include "openblas_utest.h"

/*
void BLASFUNC(cpotrf)(char*, BLASINT*, complex float*, BLASINT*, BLASINT*);
void BLASFUNC(zpotrs_(char*, BLASINT*, BLASINT*, complex double*,
	     BLASINT*, complex double*, BLASINT*, BLASINT*);
*/


//https://github.com/xianyi/OpenBLAS/issues/695
CTEST(potrf, bug_695){

  openblas_complex_float A1[100] = 
  {
    openblas_make_complex_float(5.8525753, +0.0),
    openblas_make_complex_float(-0.79540455, -0.7066077),
    openblas_make_complex_float(0.98274714, -1.3824869),
    openblas_make_complex_float(2.619998, -1.8532984),
    openblas_make_complex_float(-1.8306153, +1.2336911),
    openblas_make_complex_float(0.32275113, -0.015575029),
    openblas_make_complex_float(2.1968813, -1.0640624),
    openblas_make_complex_float(0.27894387, -0.97911835),
    openblas_make_complex_float(3.0476584, -0.18548489),
    openblas_make_complex_float(0.3842994, -0.7050991),
    openblas_make_complex_float(-0.79540455, +0.7066077),
    openblas_make_complex_float(8.313246, +0.0),
    openblas_make_complex_float(-1.8076122, +0.8882447),
    openblas_make_complex_float(0.47806996, -0.48494184),
    openblas_make_complex_float(0.5096429, +0.5395974),
    openblas_make_complex_float(-0.7285097, +0.10360408),
    openblas_make_complex_float(-1.1760061, +2.7146957),
    openblas_make_complex_float(-0.4271084, -0.042899966),
    openblas_make_complex_float(-1.7228563, -2.8335886),
    openblas_make_complex_float(1.8942566, -0.6389735),
    openblas_make_complex_float(0.98274714, +1.3824869),
    openblas_make_complex_float(-1.8076122, -0.8882447),
    openblas_make_complex_float(9.367975, +0.0),
    openblas_make_complex_float(-0.1838578, -0.6468568),
    openblas_make_complex_float(-1.8338387, -0.7064959),
    openblas_make_complex_float(0.041852742, +0.6556877),
    openblas_make_complex_float(2.5673025, -1.9732997),
    openblas_make_complex_float(-1.1148382, +0.15693812),
    openblas_make_complex_float(2.4704504, +1.0389464),
    openblas_make_complex_float(1.0858271, +1.298006),
    openblas_make_complex_float(2.619998, +1.8532984),
    openblas_make_complex_float(0.47806996, +0.48494184),
    openblas_make_complex_float(-0.1838578, +0.6468568),
    openblas_make_complex_float(3.1117508, +0.0),
    openblas_make_complex_float(-1.956626, -0.22825956),
    openblas_make_complex_float(0.07081801, +0.31801307),
    openblas_make_complex_float(0.3698375, +0.5400855),
    openblas_make_complex_float(0.80686307, -1.5315914),
    openblas_make_complex_float(1.5649154, +1.6229297),
    openblas_make_complex_float(-0.112077385, -1.2014246),
    openblas_make_complex_float(-1.8306153, -1.2336911),
    openblas_make_complex_float(0.5096429, -0.5395974),
    openblas_make_complex_float(-1.8338387, +0.7064959),
    openblas_make_complex_float(-1.956626, +0.22825956),
    openblas_make_complex_float(3.6439795, +0.0),
    openblas_make_complex_float(-0.2594722, -0.48786148),
    openblas_make_complex_float(-0.47636223, +0.27821827),
    openblas_make_complex_float(-0.61608654, +2.01858),
    openblas_make_complex_float(-2.7767487, -1.7693765),
    openblas_make_complex_float(0.048102796, +0.9741874),
    openblas_make_complex_float(0.32275113, +0.015575029),
    openblas_make_complex_float(-0.7285097, -0.10360408),
    openblas_make_complex_float(0.041852742, -0.6556877),
    openblas_make_complex_float(0.07081801, -0.31801307),
    openblas_make_complex_float(-0.2594722, +0.48786148),
    openblas_make_complex_float(3.624376, +0.0),
    openblas_make_complex_float(-1.6697118, -0.4017511),
    openblas_make_complex_float(-1.4397877, +0.7550918),
    openblas_make_complex_float(-0.31456697, +1.0403451),
    openblas_make_complex_float(-0.31978557, -0.13701046),
    openblas_make_complex_float(2.1968813, +1.0640624),
    openblas_make_complex_float(-1.1760061, -2.7146957),
    openblas_make_complex_float(2.5673025, +1.9732997),
    openblas_make_complex_float(0.3698375, -0.5400855),
    openblas_make_complex_float(-0.47636223, -0.27821827),
    openblas_make_complex_float(-1.6697118, +0.4017511),
    openblas_make_complex_float(6.8273163, +0.0),
    openblas_make_complex_float(-0.10051322, -0.24303961),
    openblas_make_complex_float(1.4415971, -0.29750675),
    openblas_make_complex_float(1.221786, +0.85654986),
    openblas_make_complex_float(0.27894387, +0.97911835),
    openblas_make_complex_float(-0.4271084, +0.042899966),
    openblas_make_complex_float(-1.1148382, -0.15693812),
    openblas_make_complex_float(0.80686307, +1.5315914),
    openblas_make_complex_float(-0.61608654, -2.01858),
    openblas_make_complex_float(-1.4397877, -0.7550918),
    openblas_make_complex_float(-0.10051322, +0.24303961),
    openblas_make_complex_float(3.4057708, +0.0),
    openblas_make_complex_float(-0.5856801, +1.0203559),
    openblas_make_complex_float(0.7103452, -0.8422135),
    openblas_make_complex_float(3.0476584, +0.18548489),
    openblas_make_complex_float(-1.7228563, +2.8335886),
    openblas_make_complex_float(2.4704504, -1.0389464),
    openblas_make_complex_float(1.5649154, -1.6229297),
    openblas_make_complex_float(-2.7767487, +1.7693765),
    openblas_make_complex_float(-0.31456697, -1.0403451),
    openblas_make_complex_float(1.4415971, +0.29750675),
    openblas_make_complex_float(-0.5856801, -1.0203559),
    openblas_make_complex_float(7.005772, +0.0),
    openblas_make_complex_float(-0.9617417, +1.2486815),
    openblas_make_complex_float(0.3842994, +0.7050991),
    openblas_make_complex_float(1.8942566, +0.6389735),
    openblas_make_complex_float(1.0858271, -1.298006),
    openblas_make_complex_float(-0.112077385, +1.2014246),
    openblas_make_complex_float(0.048102796, -0.9741874),
    openblas_make_complex_float(-0.31978557, +0.13701046),
    openblas_make_complex_float(1.221786, -0.85654986),
    openblas_make_complex_float(0.7103452, +0.8422135),
    openblas_make_complex_float(-0.9617417, -1.2486815),
    openblas_make_complex_float(3.4629636, +0.0)
  };

  char up = 'U';

  blasint n=10;
  blasint info[1];
  BLASFUNC(cpotrf)(&up, &n, (float*)(A1), &n, info);
  //printf("%g+%g*I\n", creal(A1[91]), cimag(A1[91]));

  openblas_complex_double A2[100] = 
  {
    openblas_make_complex_double(3.0607147216796875, +0.0),
    openblas_make_complex_double(-0.5905849933624268, -0.29020825028419495),
    openblas_make_complex_double(0.321084201335907, +0.45168760418891907),
    openblas_make_complex_double(0.8387917876243591, -0.644718587398529),
    openblas_make_complex_double(-0.3642411530017853, +0.051274992525577545),
    openblas_make_complex_double(0.8071482181549072, +0.33944568037986755),
    openblas_make_complex_double(0.013674172572791576, +0.21422699093818665),
    openblas_make_complex_double(0.35476258397102356, +0.42408594489097595),
    openblas_make_complex_double(-0.5991537570953369, -0.23082709312438965),
    openblas_make_complex_double(-0.0600702166557312, -0.2113417387008667),
    openblas_make_complex_double(-0.7954045534133911, +0.7066076993942261),
    openblas_make_complex_double(2.807175397872925, +0.0),
    openblas_make_complex_double(-0.1691000759601593, +0.313548743724823),
    openblas_make_complex_double(-0.30911174416542053, +0.7447023987770081),
    openblas_make_complex_double(-0.22347848117351532, +0.03316075727343559),
    openblas_make_complex_double(-0.4088296890258789, -1.0214389562606812),
    openblas_make_complex_double(-0.2344931811094284, +0.08056317269802094),
    openblas_make_complex_double(0.793269693851471, -0.17507623136043549),
    openblas_make_complex_double(0.03163455054163933, +0.20559945702552795),
    openblas_make_complex_double(0.13581633567810059, -0.2110036462545395),
    openblas_make_complex_double(0.9827471375465393, +1.3824869394302368),
    openblas_make_complex_double(-1.8076121807098389, -0.8882446885108948),
    openblas_make_complex_double(2.3277781009674072, +0.0),
    openblas_make_complex_double(0.830405056476593, -0.19296252727508545),
    openblas_make_complex_double(0.1394239068031311, -0.5260677933692932),
    openblas_make_complex_double(1.239942193031311, -0.09915469586849213),
    openblas_make_complex_double(0.06731037050485611, -0.059320636093616486),
    openblas_make_complex_double(0.11507681757211685, -0.1984301060438156),
    openblas_make_complex_double(-0.6843825578689575, +0.4647614359855652),
    openblas_make_complex_double(1.213119387626648, -0.7757048010826111),
    openblas_make_complex_double(2.619997978210449, +1.8532984256744385),
    openblas_make_complex_double(0.4780699610710144, +0.48494184017181396),
    openblas_make_complex_double(-0.18385779857635498, +0.6468567848205566),
    openblas_make_complex_double(2.0811400413513184, +0.0),
    openblas_make_complex_double(-0.035075582563877106, +0.09732913225889206),
    openblas_make_complex_double(0.27337002754211426, -0.9032229781150818),
    openblas_make_complex_double(-0.8374675512313843, +0.0479498989880085),
    openblas_make_complex_double(0.6916252374649048, +0.45711082220077515),
    openblas_make_complex_double(0.1883818507194519, +0.06482727080583572),
    openblas_make_complex_double(-0.32384994626045227, +0.05857187137007713),
    openblas_make_complex_double(-1.8306152820587158, -1.2336910963058472),
    openblas_make_complex_double(0.5096428990364075, -0.5395973920822144),
    openblas_make_complex_double(-1.833838701248169, +0.7064958810806274),
    openblas_make_complex_double(-1.956626057624817, +0.22825956344604492),
    openblas_make_complex_double(1.706615924835205, +0.0),
    openblas_make_complex_double(-0.2895336151123047, +0.17579378187656403),
    openblas_make_complex_double(-0.923172116279602, -0.4530014097690582),
    openblas_make_complex_double(0.5040621757507324, -0.37026339769363403),
    openblas_make_complex_double(-0.2824432849884033, -1.0374568700790405),
    openblas_make_complex_double(0.1399831622838974, +0.4977008104324341),
    openblas_make_complex_double(0.32275113463401794, +0.015575028955936432),
    openblas_make_complex_double(-0.7285097241401672, -0.10360407829284668),
    openblas_make_complex_double(0.041852742433547974, -0.655687689781189),
    openblas_make_complex_double(0.07081800699234009, -0.318013072013855),
    openblas_make_complex_double(-0.25947219133377075, +0.4878614842891693),
    openblas_make_complex_double(1.5735365152359009, +0.0),
    openblas_make_complex_double(-0.2647853195667267, -0.26654252409935),
    openblas_make_complex_double(-0.6190430521965027, -0.24699924886226654),
    openblas_make_complex_double(-0.6288471221923828, +0.48154571652412415),
    openblas_make_complex_double(0.02446540631353855, -0.2611822783946991),
    openblas_make_complex_double(2.1968812942504883, +1.0640623569488525),
    openblas_make_complex_double(-1.1760060787200928, -2.714695692062378),
    openblas_make_complex_double(2.5673024654388428, +1.9732997417449951),
    openblas_make_complex_double(0.3698374927043915, -0.54008549451828),
    openblas_make_complex_double(-0.4763622283935547, -0.27821826934814453),
    openblas_make_complex_double(-1.6697118282318115, +0.4017511010169983),
    openblas_make_complex_double(1.2674795389175415, +0.0),
    openblas_make_complex_double(0.3079095482826233, -0.07258892804384232),
    openblas_make_complex_double(-0.5929520130157471, -0.038360968232154846),
    openblas_make_complex_double(0.04388086497783661, -0.025549031794071198),
    openblas_make_complex_double(0.27894386649131775, +0.9791183471679688),
    openblas_make_complex_double(-0.42710840702056885, +0.0428999662399292),
    openblas_make_complex_double(-1.1148382425308228, -0.1569381207227707),
    openblas_make_complex_double(0.8068630695343018, +1.5315914154052734),
    openblas_make_complex_double(-0.6160865426063538, -2.0185799598693848),
    openblas_make_complex_double(-1.439787745475769, -0.7550917863845825),
    openblas_make_complex_double(-0.10051321983337402, +0.24303960800170898),
    openblas_make_complex_double(0.9066106081008911, +0.0),
    openblas_make_complex_double(0.05315789580345154, -0.06136537343263626),
    openblas_make_complex_double(-0.21304509043693542, +0.6494344472885132),
    openblas_make_complex_double(3.0476584434509277, +0.1854848861694336),
    openblas_make_complex_double(-1.7228562831878662, +2.8335886001586914),
    openblas_make_complex_double(2.4704504013061523, -1.0389463901519775),
    openblas_make_complex_double(1.564915418624878, -1.6229296922683716),
    openblas_make_complex_double(-2.7767486572265625, +1.769376516342163),
    openblas_make_complex_double(-0.314566969871521, -1.0403450727462769),
    openblas_make_complex_double(1.4415971040725708, +0.29750674962997437),
    openblas_make_complex_double(-0.5856801271438599, -1.0203559398651123),
    openblas_make_complex_double(0.5668219923973083, +0.0),
    openblas_make_complex_double(0.033351436257362366, -0.07832501083612442),
    openblas_make_complex_double(0.3842993974685669, +0.7050991058349609),
    openblas_make_complex_double(1.894256591796875, +0.6389734745025635),
    openblas_make_complex_double(1.085827112197876, -1.2980060577392578),
    openblas_make_complex_double(-0.11207738518714905, +1.2014245986938477),
    openblas_make_complex_double(0.04810279607772827, -0.9741873741149902),
    openblas_make_complex_double(-0.31978556513786316, +0.13701045513153076),
    openblas_make_complex_double(1.2217860221862793, -0.856549859046936),
    openblas_make_complex_double(0.7103452086448669, +0.84221351146698),
    openblas_make_complex_double(-0.9617416858673096, -1.2486815452575684),
    openblas_make_complex_double(0.0756804421544075, +0.0)
  };
  openblas_complex_double B[20] = 
  {
    openblas_make_complex_double(-0.21782716937787788, -0.9222220085490986),
    openblas_make_complex_double(-0.7620356655676837, +0.15533508334193666),
    openblas_make_complex_double(-0.905011814118756, +0.2847570854574069),
    openblas_make_complex_double(-0.3451346708401685, +1.076948486041297),
    openblas_make_complex_double(0.25336108035924787, +0.975317836492159),
    openblas_make_complex_double(0.11192755545114, -0.1603741874112385),
    openblas_make_complex_double(-0.20604111555491242, +0.10570814584017311),
    openblas_make_complex_double(-1.0568488936791578, -0.06025820467086475),
    openblas_make_complex_double(-0.6650468984506477, -0.5000967284800251),
    openblas_make_complex_double(-1.0509472322215125, +0.5022165705328413),
    openblas_make_complex_double(-0.727775859267237, +0.50638268521728),
    openblas_make_complex_double(0.39947219167701153, -0.4576746001199889),
    openblas_make_complex_double(-0.7122162951294634, -0.630289556702497),
    openblas_make_complex_double(0.9870834574024372, -0.2825689605519449),
    openblas_make_complex_double(0.0628393808469436, -0.1253397353973715),
    openblas_make_complex_double(0.8439562576196216, +1.0850814110398734),
    openblas_make_complex_double(0.562377322638969, -0.2578030745663871),
    openblas_make_complex_double(0.12696236014017806, -0.09853584666755086),
    openblas_make_complex_double(-0.023682508769195098, +0.18093440285319276),
    openblas_make_complex_double(-0.7264975746431271, +0.31670415674097235)
  };
  char lo = 'L';
  blasint nrhs = 2;
  BLASFUNC(zpotrs)(&lo, &n, &nrhs, (double*)(A2), &n, (double*)(B), &n, info);

  // note that this is exactly equal to A1
  openblas_complex_float A3[100] = 
  {
    openblas_make_complex_float(5.8525753, +0.0),
    openblas_make_complex_float(-0.79540455, -0.7066077),
    openblas_make_complex_float(0.98274714, -1.3824869),
    openblas_make_complex_float(2.619998, -1.8532984),
    openblas_make_complex_float(-1.8306153, +1.2336911),
    openblas_make_complex_float(0.32275113, -0.015575029),
    openblas_make_complex_float(2.1968813, -1.0640624),
    openblas_make_complex_float(0.27894387, -0.97911835),
    openblas_make_complex_float(3.0476584, -0.18548489),
    openblas_make_complex_float(0.3842994, -0.7050991),
    openblas_make_complex_float(-0.79540455, +0.7066077),
    openblas_make_complex_float(8.313246, +0.0),
    openblas_make_complex_float(-1.8076122, +0.8882447),
    openblas_make_complex_float(0.47806996, -0.48494184),
    openblas_make_complex_float(0.5096429, +0.5395974),
    openblas_make_complex_float(-0.7285097, +0.10360408),
    openblas_make_complex_float(-1.1760061, +2.7146957),
    openblas_make_complex_float(-0.4271084, -0.042899966),
    openblas_make_complex_float(-1.7228563, -2.8335886),
    openblas_make_complex_float(1.8942566, -0.6389735),
    openblas_make_complex_float(0.98274714, +1.3824869),
    openblas_make_complex_float(-1.8076122, -0.8882447),
    openblas_make_complex_float(9.367975, +0.0),
    openblas_make_complex_float(-0.1838578, -0.6468568),
    openblas_make_complex_float(-1.8338387, -0.7064959),
    openblas_make_complex_float(0.041852742, +0.6556877),
    openblas_make_complex_float(2.5673025, -1.9732997),
    openblas_make_complex_float(-1.1148382, +0.15693812),
    openblas_make_complex_float(2.4704504, +1.0389464),
    openblas_make_complex_float(1.0858271, +1.298006),
    openblas_make_complex_float(2.619998, +1.8532984),
    openblas_make_complex_float(0.47806996, +0.48494184),
    openblas_make_complex_float(-0.1838578, +0.6468568),
    openblas_make_complex_float(3.1117508, +0.0),
    openblas_make_complex_float(-1.956626, -0.22825956),
    openblas_make_complex_float(0.07081801, +0.31801307),
    openblas_make_complex_float(0.3698375, +0.5400855),
    openblas_make_complex_float(0.80686307, -1.5315914),
    openblas_make_complex_float(1.5649154, +1.6229297),
    openblas_make_complex_float(-0.112077385, -1.2014246),
    openblas_make_complex_float(-1.8306153, -1.2336911),
    openblas_make_complex_float(0.5096429, -0.5395974),
    openblas_make_complex_float(-1.8338387, +0.7064959),
    openblas_make_complex_float(-1.956626, +0.22825956),
    openblas_make_complex_float(3.6439795, +0.0),
    openblas_make_complex_float(-0.2594722, -0.48786148),
    openblas_make_complex_float(-0.47636223, +0.27821827),
    openblas_make_complex_float(-0.61608654, +2.01858),
    openblas_make_complex_float(-2.7767487, -1.7693765),
    openblas_make_complex_float(0.048102796, +0.9741874),
    openblas_make_complex_float(0.32275113, +0.015575029),
    openblas_make_complex_float(-0.7285097, -0.10360408),
    openblas_make_complex_float(0.041852742, -0.6556877),
    openblas_make_complex_float(0.07081801, -0.31801307),
    openblas_make_complex_float(-0.2594722, +0.48786148),
    openblas_make_complex_float(3.624376, +0.0),
    openblas_make_complex_float(-1.6697118, -0.4017511),
    openblas_make_complex_float(-1.4397877, +0.7550918),
    openblas_make_complex_float(-0.31456697, +1.0403451),
    openblas_make_complex_float(-0.31978557, -0.13701046),
    openblas_make_complex_float(2.1968813, +1.0640624),
    openblas_make_complex_float(-1.1760061, -2.7146957),
    openblas_make_complex_float(2.5673025, +1.9732997),
    openblas_make_complex_float(0.3698375, -0.5400855),
    openblas_make_complex_float(-0.47636223, -0.27821827),
    openblas_make_complex_float(-1.6697118, +0.4017511),
    openblas_make_complex_float(6.8273163, +0.0),
    openblas_make_complex_float(-0.10051322, -0.24303961),
    openblas_make_complex_float(1.4415971, -0.29750675),
    openblas_make_complex_float(1.221786, +0.85654986),
    openblas_make_complex_float(0.27894387, +0.97911835),
    openblas_make_complex_float(-0.4271084, +0.042899966),
    openblas_make_complex_float(-1.1148382, -0.15693812),
    openblas_make_complex_float(0.80686307, +1.5315914),
    openblas_make_complex_float(-0.61608654, -2.01858),
    openblas_make_complex_float(-1.4397877, -0.7550918),
    openblas_make_complex_float(-0.10051322, +0.24303961),
    openblas_make_complex_float(3.4057708, +0.0),
    openblas_make_complex_float(-0.5856801, +1.0203559),
    openblas_make_complex_float(0.7103452, -0.8422135),
    openblas_make_complex_float(3.0476584, +0.18548489),
    openblas_make_complex_float(-1.7228563, +2.8335886),
    openblas_make_complex_float(2.4704504, -1.0389464),
    openblas_make_complex_float(1.5649154, -1.6229297),
    openblas_make_complex_float(-2.7767487, +1.7693765),
    openblas_make_complex_float(-0.31456697, -1.0403451),
    openblas_make_complex_float(1.4415971, +0.29750675),
    openblas_make_complex_float(-0.5856801, -1.0203559),
    openblas_make_complex_float(7.005772, +0.0),
    openblas_make_complex_float(-0.9617417, +1.2486815),
    openblas_make_complex_float(0.3842994, +0.7050991),
    openblas_make_complex_float(1.8942566, +0.6389735),
    openblas_make_complex_float(1.0858271, -1.298006),
    openblas_make_complex_float(-0.112077385, +1.2014246),
    openblas_make_complex_float(0.048102796, -0.9741874),
    openblas_make_complex_float(-0.31978557, +0.13701046),
    openblas_make_complex_float(1.221786, -0.85654986),
    openblas_make_complex_float(0.7103452, +0.8422135),
    openblas_make_complex_float(-0.9617417, -1.2486815),
    openblas_make_complex_float(3.4629636, +0.0)
  };
  BLASFUNC(cpotrf)(&up, &n, (float*)(A3), &n, info);
  //  printf("%g+%g*I\n", creal(A3[91]), cimag(A3[91]));
  if(isnan(CREAL(A3[91])) || isnan(CIMAG(A3[91]))) {
    CTEST_ERR("%s:%d  got NaN", __FILE__, __LINE__);
  }
}


// Check potrf factorizes a small problem correctly
CTEST(potrf, smoketest_trivial){
  float A1s[4] = {2, 0.3, 0.3, 3};
  double A1d[4] = {2, 0.3, 0.3, 3};
  openblas_complex_float A1c[4] = {
    openblas_make_complex_float(2,0),
    openblas_make_complex_float(0.3,0.1),
    openblas_make_complex_float(0.3,-0.1),
    openblas_make_complex_float(3,0)
  };
  openblas_complex_double A1z[4] = {
    openblas_make_complex_double(2,0),
    openblas_make_complex_double(0.3,0.1),
    openblas_make_complex_double(0.3,-0.1),
    openblas_make_complex_double(3,0)
  };
  float zeros = 0, ones = 1;
  double zerod = 0, oned = 1;
  openblas_complex_float zeroc = openblas_make_complex_float(0, 0),
                         onec = openblas_make_complex_float(1, 0);
  openblas_complex_double zeroz = openblas_make_complex_double(0, 0),
                          onez = openblas_make_complex_float(1, 0);

  char uplo, trans1, trans2;
  blasint nv = 4;
  blasint n = 2;
  blasint inc = 1;
  blasint info = 0;
  int i, j, cycle;

  float As[4], Bs[4];
  double Ad[4], Bd[4];
  openblas_complex_float Ac[4], Bc[4];
  openblas_complex_double Az[4], Bz[4];

  for (cycle = 0; cycle < 2; ++cycle) {
    if (cycle == 0) {
      uplo = 'L';
    }
    else {
      uplo = 'U';
    }

    BLASFUNC(scopy)(&nv, A1s, &inc, As, &inc);
    BLASFUNC(dcopy)(&nv, A1d, &inc, Ad, &inc);
    BLASFUNC(ccopy)(&nv, (float *)A1c, &inc, (float *)Ac, &inc);
    BLASFUNC(zcopy)(&nv, (double *)A1z, &inc, (double *)Az, &inc);

    BLASFUNC(spotrf)(&uplo, &n, As, &n, &info);
    if (info != 0) {
      CTEST_ERR("%s:%d  info != 0", __FILE__, __LINE__);
    }

    BLASFUNC(dpotrf)(&uplo, &n, Ad, &n, &info);
    if (info != 0) {
      CTEST_ERR("%s:%d  info != 0", __FILE__, __LINE__);
    }

    BLASFUNC(cpotrf)(&uplo, &n, (float *)Ac, &n, &info);
    if (info != 0) {
      CTEST_ERR("%s:%d  info != 0", __FILE__, __LINE__);
    }

    BLASFUNC(zpotrf)(&uplo, &n, (double *)Az, &n, &info);
    if (info != 0) {
      CTEST_ERR("%s:%d  info != 0", __FILE__, __LINE__);
    }

    /* Fill the other triangle */
    if (uplo == 'L') {
      for (i = 0; i < n; ++i) {
        for (j = i+1; j < n; ++j) {
          As[i+n*j] = 0;
          Ad[i+n*j] = 0;
          Ac[i+n*j] = zeroc;
          Az[i+n*j] = zeroz;
        }
      }
    }
    else {
      for (i = 0; i < n; ++i) {
        for (j = 0; j < i; ++j) {
          As[i+n*j] = 0;
          Ad[i+n*j] = 0;
          Ac[i+n*j] = zeroc;
          Az[i+n*j] = zeroz;
        }
      }
    }

    /* B = A A^H or A^H A */
    if (uplo == 'L') {
      trans1 = 'N';
      trans2 = 'C';
    }
    else {
      trans1 = 'C';
      trans2 = 'N';
    }

    BLASFUNC(sgemm)(&trans1, &trans2, &n, &n, &n, &ones, As, &n, As, &n, &zeros, Bs, &n);
    BLASFUNC(dgemm)(&trans1, &trans2, &n, &n, &n, &oned, Ad, &n, Ad, &n, &zerod, Bd, &n);
    BLASFUNC(cgemm)(&trans1, &trans2, &n, &n, &n, (float *)&onec,
                    (float *)Ac, &n, (float *)Ac, &n, (float *)&zeroc, (float *)Bc, &n);
    BLASFUNC(zgemm)(&trans1, &trans2, &n, &n, &n, (double *)&onez,
                    (double *)Az, &n, (double *)Az, &n, (double *)&zeroz, (double *)Bz, &n);

    /* Check result is close to original */
    for (i = 0; i < n; ++i) {
      for (j = 0; j < n; ++j) {
        double err;

        err = fabs(A1s[i+n*j] - Bs[i+n*j]);
        if (err > 1e-5) {
          CTEST_ERR("%s:%d  %c s(%d,%d) difference: %g", __FILE__, __LINE__, uplo, i, j, err);
        }

        err = fabs(A1d[i+n*j] - Bd[i+n*j]);
        if (err > 1e-12) {
          CTEST_ERR("%s:%d  %c d(%d,%d) difference: %g", __FILE__, __LINE__, uplo, i, j, err);
        }

#ifdef OPENBLAS_COMPLEX_C99
        err = cabsf(A1c[i+n*j] - Bc[i+n*j]);
#else
        err = hypot(A1c[i+n*j].real - Bc[i+n*j].real,
                    A1c[i+n*j].imag - Bc[i+n*j].imag);
#endif
        if (err > 1e-5) {
          CTEST_ERR("%s:%d  %c c(%d,%d) difference: %g", __FILE__, __LINE__, uplo, i, j, err);
        }

#ifdef OPENBLAS_COMPLEX_C99
        err = cabs(A1z[i+n*j] - Bz[i+n*j]);
#else
        err = hypot(A1z[i+n*j].real - Bz[i+n*j].real,
                    A1z[i+n*j].imag - Bz[i+n*j].imag);
#endif
        if (err > 1e-12) {
          CTEST_ERR("%s:%d  %c z(%d,%d) difference: %g", __FILE__, __LINE__, uplo, i, j, err);
        }
      }
    }
  }
}
