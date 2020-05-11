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

package biggrph._algo.count;

import java.io.Serializable;

@SuppressWarnings("serial")
public class EdgeAndVertexCount implements Serializable
{
	public long nbVertices;
	public long nbEdges;

	@Override
	public String toString()
	{
		return "NumberOfEdgesAndVertices [nbVertices=" + nbVertices + ", nbEdges="
				+ nbEdges + "]";
	}
}