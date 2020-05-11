/////////////////////////////////////////////////////////////////////////////////////////
// 
//                 Université de Nice Sophia-Antipolis  (UNS) - 
//                 Centre National de la Recherche Scientifique (CNRS)
//                 Copyright © 2015 UNS, CNRS All Rights Reserved.
// 
//     These computer program listings and specifications, herein, are
//     the property of Université de Nice Sophia-Antipolis and CNRS
//     shall not be reproduced or copied or used in whole or in part as
//     the basis for manufacture or sale of items without written permission.
//     For a license agreement, please contact:
//     <mailto: licensing@sattse.com> 
//
//
//
//     Author: Luc Hogie – Laboratoire I3S - luc.hogie@unice.fr
//
//////////////////////////////////////////////////////////////////////////////////////////

package biggrph.dataset;

import java.io.BufferedReader;
import java.io.IOException;

import toools.io.file.RegularFile;

public class DataSetAnalyzer
{
	public static void main(String[] args) throws IOException
	{
		System.out.println(analyze(new RegularFile("$HOME/datasets/twitter_small.tsv")));
	}

	public static Result analyze(RegularFile f) throws IOException
	{
		BufferedReader is = f.createLineReadingStream();
		String line = null;

		Result r = new Result();

		while ((line = is.readLine()) != null)
		{
			++r.numberOfLines;

			for (String t : line.split("\t"))
			{
				long n = Long.valueOf(t);
				++r.numberOfNumbers;

				if (n > r.max)
				{
					r.max = n;
				}
			}
		}

		is.close();
		return r;
	}

	public static class Result
	{
		long max = 0;
		long numberOfLines = 0;
		long numberOfNumbers = 0;

		@Override
		public String toString()
		{
			return "Result [max=" + max + ", numberOfLines=" + numberOfLines + ", numberOfNumbers=" + numberOfNumbers + "]";
		}

		
		public boolean requiredLongValues()
		{
			return max > Integer.MAX_VALUE;
		}
	}
}
