using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text;

namespace BaselineBuilder.ICDHandling
{
    public static class BitMaskService
    {
        private const int _floatSize = 8;
        private const int _byteBits = 8;

        public static double GetFloat64(byte[] data, byte[] mask)
        {
            byte[] result = new byte[_floatSize];

            if (data.Length != _floatSize)
            {
                int resultBit = _floatSize * _byteBits;

                for (int i = data.Length - 1; i >= 0; i--)
                {
                    for (int j = 0; j < _byteBits; j++)
                    {
                        if (((mask[i] >> j) & 1) == 1)
                        {
                            resultBit--;

                            if (((data[i] >> j) & 1) == 1)
                            {
                                int index = resultBit / _byteBits;
                                int bit = resultBit % _byteBits;

                                result[index] |= (byte)(1 << bit);
                            }
                        }
                    }
                }
            }
            else
            {
                for (int i = 0; i < _floatSize; i++)
                {
                    result[i] = (byte)(data[i] & mask[i]);
                }
            }

            Array.Reverse(result);

            return BitConverter.ToDouble(result);
        }

        public static int GetInt32(byte[] data, byte[] mask, ParameterDataType type)
        {
            int lowBitsEnd = 0;

            while (((mask[mask.Length - 1] >> lowBitsEnd) & 1) == 0)
            {
                lowBitsEnd++;
            }

            bool unsigned = true;
            if (type == ParameterDataType.Int16 || type == ParameterDataType.Int32)
            {
                unsigned = false;
            }

            BigInteger iData = new BigInteger(data, isBigEndian: true, isUnsigned: unsigned);
            BigInteger iMask = new BigInteger(mask, isBigEndian: true, isUnsigned: unsigned);

            BigInteger masked = iData & iMask;
            BigInteger result = masked >> lowBitsEnd;

            return checked((int)(result));
        }
    }
}
