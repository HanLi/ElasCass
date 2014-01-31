/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token.TokenSerializer;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;

import static com.google.common.base.Charsets.UTF_8;

public class OrderPreservingPartitioner implements IPartitioner<StringToken>
{
    public static final StringToken MINIMUM = new StringToken("");

    public static final BigInteger CHAR_MASK = new BigInteger("65535");

    public DecoratedKey<StringToken> decorateKey(ByteBuffer key)
    {
        return new DecoratedKey<StringToken>(getToken(key), key);
    }
    
    public DecoratedKey<StringToken> convertFromDiskFormat(ByteBuffer key)
    {
        return new DecoratedKey<StringToken>(getToken(key), key);
    }

    public StringToken midpoint(Token ltoken, Token rtoken)
    {
        int sigchars = Math.max(((StringToken)ltoken).token.length(), ((StringToken)rtoken).token.length());
        BigInteger left = bigForString(((StringToken)ltoken).token, sigchars);
        BigInteger right = bigForString(((StringToken)rtoken).token, sigchars);

        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(left, right, 16*sigchars);
        return new StringToken(stringForBig(midpair.left, sigchars, midpair.right));
    }

    /**
     * Copies the characters of the given string into a BigInteger.
     *
     * TODO: Does not acknowledge any codepoints above 0xFFFF... problem?
     */
    private static BigInteger bigForString(String str, int sigchars)
    {
        assert str.length() <= sigchars;

        BigInteger big = BigInteger.ZERO;
        for (int i = 0; i < str.length(); i++)
        {
            int charpos = 16 * (sigchars - (i + 1));
            BigInteger charbig = BigInteger.valueOf(str.charAt(i) & 0xFFFF);
            big = big.or(charbig.shiftLeft(charpos));
        }
        return big;
    }

    /**
     * Convert a (positive) BigInteger into a String.
     * If remainder is true, an additional char with the high order bit enabled
     * will be added to the end of the String.
     */
    private String stringForBig(BigInteger big, int sigchars, boolean remainder)
    {
        char[] chars = new char[sigchars + (remainder ? 1 : 0)];
        if (remainder)
            // remaining bit is the most significant in the last char
            chars[sigchars] |= 0x8000;
        for (int i = 0; i < sigchars; i++)
        {
            int maskpos = 16 * (sigchars - (i + 1));
            // apply bitmask and get char value
            chars[i] = (char)(big.and(CHAR_MASK.shiftLeft(maskpos)).shiftRight(maskpos).intValue() & 0xFFFF);
        }
        return new String(chars);
    }

    public StringToken getMinimumToken()
    {
        return MINIMUM;
    }

    public StringToken getRandomToken()
    {
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random r = new Random();
        StringBuilder buffer = new StringBuilder();
        for (int j = 0; j < 16; j++) {
            buffer.append(chars.charAt(r.nextInt(chars.length())));
        }
        return new StringToken(buffer.toString());
    }

    private final Token.TokenFactory<String> tokenFactory = new Token.TokenFactory<String>()
    {
        public ByteBuffer toByteArray(Token<String> stringToken)
        {
            return ByteBufferUtil.bytes(stringToken.token);
        }

        public Token<String> fromByteArray(ByteBuffer bytes)
        {
            try
            {
                return new StringToken(ByteBufferUtil.string(bytes));
            }
            catch (CharacterCodingException e)
            {
                throw new RuntimeException(e);
            }
        }

        public String toString(Token<String> stringToken)
        {
            return stringToken.token;
        }

        public void validate(String token) throws ConfigurationException
        {
            if (token.contains(VersionedValue.DELIMITER_STR))
                throw new ConfigurationException("Tokens may not contain the character " + VersionedValue.DELIMITER_STR);
        }

        public Token<String> fromString(String string)
        {
            return new StringToken(string);
        }

		@Override
		public ByteBuffer toByteArray(Collection<Token<String>> tokens) 
		{
			int size = 0;
			for(Token<String> stringToken : tokens)
			{
				size += 4 + stringToken.token.getBytes(UTF_8).length;
			}
			
			ByteBuffer byteBuffer = ByteBuffer.allocate(size);
			for(Token<String> stringToken : tokens)
			{
				byteBuffer.putInt(stringToken.token.getBytes(UTF_8).length);
				byteBuffer.put(stringToken.token.getBytes(UTF_8));
			}
			
			byteBuffer.position(0);
			return byteBuffer;
		}

		@Override
		public Collection<Token<String>> fromByteArrayToCollection(ByteBuffer bytes) 
		{
			Set<Token<String>> tokens = new HashSet<Token<String>>();
			while(bytes.remaining()>0)
			{
				int length = bytes.getInt();
				byte[] array = new byte[length];
				bytes.get(array);
				Token<String> stringToken = new StringToken(new String(array, UTF_8));
				tokens.add(stringToken);
			}
			return tokens;
		}

		@Override
		public String toString(Collection<Token<String>> tokens) 
		{
			StringBuilder sb = new StringBuilder();
			for(Token<String> stringToken : tokens)
			{
				sb.append(stringToken.token);
				sb.append(TokenSerializer.DELIMITER_TOKEN);
			}
			if(sb.toString().charAt(sb.length()-1)==TokenSerializer.DELIMITER_TOKEN) 
			{
        		sb.deleteCharAt(sb.length()-1);
        	}
			return sb.toString();
		}

		@Override
		public Collection<Token<String>> fromStringToCollection(String string) 
		{
			String[] strs = string.split(TokenSerializer.DELIMITER_TOKEN_STR, -1);
			Set<Token<String>> set = new HashSet<Token<String>>();
			for(String str : strs) if(str.length() > 0)
			{
				set.add(new StringToken(str));
			}
			return set;
		}
    };

    public Token.TokenFactory<String> getTokenFactory()
    {
        return tokenFactory;
    }

    public boolean preservesOrder()
    {
        return true;
    }

    public StringToken getToken(ByteBuffer key)
    {
        String skey;
        try
        {
            skey = ByteBufferUtil.string(key);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException("The provided key was not UTF8 encoded.", e);
        }
        return new StringToken(skey);
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        // allTokens will contain the count and be returned, sorted_ranges is shorthand for token<->token math.
        Map<Token, Float> allTokens = new HashMap<Token, Float>();
        List<Range> sortedRanges = new ArrayList<Range>();

        // this initializes the counts to 0 and calcs the ranges in order.
        Token lastToken = sortedTokens.get(sortedTokens.size() - 1);
        for (Token node : sortedTokens)
        {
            allTokens.put(node, new Float(0.0));
            sortedRanges.add(new Range(lastToken, node));
            lastToken = node;
        }

        for (String ks : Schema.instance.getTables())
        {
            for (CFMetaData cfmd : Schema.instance.getKSMetaData(ks).cfMetaData().values())
            {
                for (Range r : sortedRanges)
                {
                    // Looping over every KS:CF:Range, get the splits size and add it to the count
                    allTokens.put(r.right, allTokens.get(r.right) + StorageService.instance.getSplits(ks, cfmd.cfName, r, DatabaseDescriptor.getIndexInterval()).size());
                }
            }
        }

        // Sum every count up and divide count/total for the fractional ownership.
        Float total = new Float(0.0);
        for (Float f : allTokens.values())
            total += f;
        for (Map.Entry<Token, Float> row : allTokens.entrySet())
            allTokens.put(row.getKey(), row.getValue() / total);

        return allTokens;
    }
}
