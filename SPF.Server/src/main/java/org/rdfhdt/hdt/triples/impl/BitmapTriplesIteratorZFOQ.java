/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples/impl/BitmapTriplesIteratorZFOQ.java $
 * Revision: $Rev: 191 $
 * Last modified: $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $
 * Last modified by: $Author: mario.arias $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Contacting the authors:
 *   Mario Arias:               mario.arias@deri.org
 *   Javier D. Fernandez:       jfergar@infor.uva.es
 *   Miguel A. Martinez-Prieto: migumar2@infor.uva.es
 *   Alejandro Andres:          fuzzy.alej@gmail.com
 */

package org.rdfhdt.hdt.triples.impl;

import org.rdfhdt.hdt.compact.bitmap.AdjacencyList;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;

import java.util.Set;
import java.util.HashSet;


/**
 * @author mario.arias
 *
 */
public class BitmapTriplesIteratorZFOQ implements IteratorTripleID {
	final BitmapTriples triples;
	final TripleID pattern;
    final TripleID returnTriple;
	
	AdjacencyList adjY, adjIndex;
	long posIndex, minIndex, maxIndex;
	int x, y, z;
	
	int patY;
    final int patZ;

	Set<Long> uniquePredicates = new HashSet<>();
	
	/////////////////////////////////////////////////////////////////////////////////////////////////////
	public BitmapTriplesIteratorZFOQ(BitmapTriples triples, TripleID pattern) {
		this.triples = triples;
		this.pattern = new TripleID(pattern); // copies the pattern object 
		this.returnTriple = new TripleID();
		System.out.println("\nClass BitmapTripleIteratorZFOQ.java - constructor (BitmapTriples triples, TripleID pattern)");
		System.out.println("this.triples = "+triples);
		System.out.println("this.pattern = "+pattern);
		
		TripleOrderConvert.swapComponentOrder(this.pattern, TripleComponentOrder.SPO, triples.order);
		patZ = this.pattern.getObject();
		System.out.println("patZ = this.pattern.getObject() - is objectID - patZ = "+patZ);
		
		if(patZ==0 && (patY!=0 || this.pattern.getSubject()!=0)) {
			throw new IllegalArgumentException("This structure is not meant to process this pattern");
		}
		
	    patY = this.pattern.getPredicate();
		System.out.println("patY = this.pattern.getPredicate() - is predicateID - patY = "+patY);
		
		adjY = triples.adjY;
		adjIndex = triples.adjIndex;



	
		calculateRange();
		goToStart();
	}
	

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private long getY(long index) {
		System.out.println("**** index = "+index);
		System.out.println("**** adjIndex.get("+index+") = "+adjIndex.get(index));
		System.out.println("**** adjY.get(adjIndex.get(index)) = "+ adjY.get(adjIndex.get(index)));
		return adjY.get(adjIndex.get(index));
	}
	

	///////////////////////////////////////////////////////////////////////////////////////////

	private void calculateRange() {
		System.out.println("-- inside Method: calculateRange()");
		
		if(patZ == 0) {
			System.out.println("IF patZ == 0 then: ");
			minIndex = 0;
			maxIndex = adjIndex.getNumberOfElements();
			System.out.println("minIndex = 0 and maxIndex = "+adjIndex.getNumberOfElements());
			return;
		}

		minIndex = adjIndex.find(patZ-1);
		System.out.println("minIndex = adjIndex.find(patZ-1) - So minIndex = "+minIndex);
		
		maxIndex = adjIndex.last(patZ-1);
		System.out.println("maxIndex = adjIndex.last(patZ-1) - So maxIndex = "+maxIndex);
        

		if(patY!=0) {
			while (minIndex <= maxIndex) {
				System.out.println("While(minIndex <= maxIndex) - So ( "+minIndex+" <= "+maxIndex+" )" );
				long mid = (minIndex + maxIndex) / 2;
				System.out.println("long mid = (minIndex + maxIndex) / 2; - so mid = " +mid);
				long predicate = getY(mid);

				System.out.println("long predicate=getY(mid) - So predicate = "+predicate);      
				System.out.println("patY = "+patY+" and predicate = "+predicate);  

				if (patY > predicate) {
					System.out.println("if (patY > predicate) - ( "+patY+ " > "+predicate+" ) then:");
					minIndex = mid + 1;
					System.out.println("minIndex = mid + 1  so minIndex = "+minIndex);

				} else if (patY < predicate) {
					System.out.println("if (patY < predicate) - ( "+patY+" < "+predicate+ " ) then:");
					maxIndex = mid - 1;
					System.out.println("maxIndex = mid - 1 So maxIndex = "+maxIndex);

				} else {
					// Binary Search to find left boundary
					System.out.println("Binary search to find Left boundry");
					long left=minIndex;
					System.out.println("long left=minIndex so left = "+left);
					long right=mid;
					System.out.println("long right=mid so right = "+right);
					long pos=0;
					System.out.println("pos = "+pos);

					while(left<=right) {
						System.out.println("While left<=right - ( "+left+" <= "+right+" )");
						pos = (left+right)/2;
						System.out.println("pos = (left+right)/2 - so pos = "+pos);

						predicate = getY(pos);
						System.out.println("predicate = getY(pos); - so predicate = "+predicate);

						if(predicate!=patY) {
							System.out.println("if(predicate!=patY) -- i.e., ( "+predicate+" != "+patY);
							left = pos+1;
							System.out.println("left = pos + 1 - so left = "+left);
						} else {
							right = pos-1;
							System.out.println("else right = pos-1 - i.e.,  right = "+right);
						}
					}
					minIndex = predicate==patY ? pos : pos+1;
					System.out.println("minIndex = predicate==patY ? pos : pos+1; - i.e., minIndex = "+minIndex);

					// Binary Search to find right boundary
					System.out.println("Binary search to find right boundry");
					left = mid;
					System.out.println("left = mid - so left = "+left);
					right= maxIndex;
					System.out.println("right = maxIndex - so right = "+right);

					while(left<=right) {
						System.out.println("while(left<=right) - so ( "+left+" <= "+right);
						pos = (left+right)/2;
						System.out.println("pos = (left+right)/2 - so pos = "+pos);
						predicate = getY(pos);
						System.out.println("predicate = getY(pos) -- predicate = "+predicate);

						if(predicate!=patY) {
							System.out.println("if(predicate != patY) -- ("+predicate+" != "+patY+")");
							right = pos-1;
							System.out.println("right = pos-1 -- right = "+right);
						} else {
							left = pos+1;
							System.out.println(" else left = pos+1 -- i.e., left = "+left );
						}
					}
					maxIndex = predicate==patY ? pos : pos-1;
					System.out.println("maxIndex = predicate==patY ? pos : pos-1 -- so maxIndex = "+maxIndex);

					break;
				}
			}
		}
		System.out.println("-- In calculate the final Range: the minIndex = "+minIndex+" and the maxIndex = "+maxIndex);
	}
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	private void updateOutput() {
		returnTriple.setAll(x, y, z);
		TripleOrderConvert.swapComponentOrder(returnTriple, triples.order, TripleComponentOrder.SPO);
	}
	
	/* (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return posIndex<=maxIndex;
	}
	
	/* (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#next()
	 */
	@Override
	public TripleID next() {
	    long posY = adjIndex.get(posIndex);

	    z = patZ!=0 ? patZ : (int)adjIndex.findListIndex(posIndex)+1;
	    y = patY!=0 ? patY : (int) adjY.get(posY);
	    x = (int) adjY.findListIndex(posY)+1;

	    posIndex++;

	    updateOutput();
	    return returnTriple;
	}

	/* (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#hasPrevious()
	 */
	@Override
	public boolean hasPrevious() {
		return posIndex>minIndex;
	}

	/* (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#previous()
	 */
	@Override
	public TripleID previous() {
		posIndex--;

		long posY = adjIndex.get(posIndex);

		z = patZ!=0 ? patZ : (int)adjIndex.findListIndex(posIndex)+1;
		y = patY!=0 ? patY : (int) adjY.get(posY);
		x = (int) adjY.findListIndex(posY)+1;

		updateOutput();
		return returnTriple;
	}

	/* (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#goToStart()
	 */
	@Override
	public void goToStart() {
		posIndex = minIndex;
		System.out.println("- Method goToStart() --- posIndex = minIndex -- so posIndex = "+posIndex);
	}

	/* (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#estimatedNumResults()
	 */
	@Override
	public long estimatedNumResults() {
		System.out.println("inside BitmapTriplesIteratorZFOQ.java - estimatedNumResults()");
		return maxIndex-minIndex+1;
	}

	/* (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#numResultEstimation()
	 */
	@Override
	public ResultEstimationType numResultEstimation() {
	    return ResultEstimationType.EXACT;
	}

	/* (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#canGoTo()
	 */
	@Override
	public boolean canGoTo() {
		return true;
	}

	/* (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#goTo(int)
	 */
	@Override
	public void goTo(long pos) {
		if(pos>maxIndex-minIndex || pos<0) {
			throw new IndexOutOfBoundsException();
		}
		posIndex = minIndex+pos;
	}
	
	/* (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleID#getOrder()
	 */
	@Override
	public TripleComponentOrder getOrder() {
		System.out.println("Method: TripleComponenetOrder = "+triples.order);
		return triples.order;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
    
	
}
