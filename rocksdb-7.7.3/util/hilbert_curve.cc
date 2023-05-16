# include <cstdlib>
# include <ctime>
# include <iomanip>
# include <iostream>

#include "util/hilbert_curve.h"

namespace rocksdb {
    void d2xy ( int m, int d, int &x, int &y ) {
        int n;
        int rx;
        int ry;
        int s;
        int t = d;

        n = i4_power ( 2, m );

        x = 0;
        y = 0;
        for ( s = 1; s < n; s = s * 2 )
        {
            rx = ( 1 & ( t / 2 ) );
            ry = ( 1 & ( t ^ rx ) );
            rot ( s, x, y, rx, ry );
            x = x + s * rx;
            y = y + s * ry;
            t = t / 4;
        }
        return;
    }

    int i4_power ( int i, int j ) {
        int k;
        int value;

        if ( j < 0 )
        {
            if ( i == 1 )
            {
            value = 1;
            }
            else if ( i == 0 )
            {
            std::cerr << "\n";
            std::cerr << "I4_POWER - Fatal error!\n";
            std::cerr << "  I^J requested, with I = 0 and J negative.\n";
            exit ( 1 );
            }
            else
            {
            value = 0;
            }
        }
        else if ( j == 0 )
        {
            if ( i == 0 )
            {
            std::cerr << "\n";
            std::cerr << "I4_POWER - Fatal error!\n";
            std::cerr << "  I^J requested, with I = 0 and J = 0.\n";
            exit ( 1 );
            }
            else
            {
            value = 1;
            }
        }
        else if ( j == 1 )
        {
            value = i;
        }
        else
        {
            value = 1;
            for ( k = 1; k <= j; k++ )
            {
            value = value * i;
            }
        }
        return value;
    }

    void rot ( int n, int &x, int &y, int rx, int ry ) {
        int t;

        if ( ry == 0 )
        {
        //
        //  Reflect.
        //
            if ( rx == 1 )
            {
            x = n - 1 - x;
            y = n - 1 - y;
            }
        //
        //  Flip.
        //
            t = x;
            x = y;
            y = t;
        }
        return;
    }

    int xy2d ( int m, int x, int y ) {
        int d = 0;
        int n;
        int rx;
        int ry;
        int s;

        n = i4_power ( 2, m );

        for ( s = n / 2; s > 0; s = s / 2 )
        {
            rx = ( x & s ) > 0;
            ry = ( y & s ) > 0;
            d = d + s * s * ( ( 3 * rx ) ^ ry );
            rot ( s, x, y, rx, ry );
        }
        return d;
    }
}  // namespace rocksdb