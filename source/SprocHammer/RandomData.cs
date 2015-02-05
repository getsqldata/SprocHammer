using System;

namespace SprocHammer
{
    public static class RandomData
    {
        private static char[] charSet =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();

        public static string String(Random random, int minLength, int maxLength)
        {
            int length = random.Next(minLength, maxLength);
            byte[] bytes = new byte[length];
            random.NextBytes(bytes);
            char[] chars = new char[length];
            for (int i = 0; i < length; i++)
            {
                chars[i] = charSet[bytes[i] % 62];
            }
            return new string(chars);
        }
    }
}
