package org.rdfhdt.hdt.triples.impl;

import org.rdfhdt.hdt.compact.bitmap.Bitmap;
import org.rdfhdt.hdt.compact.sequence.Sequence;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.stars.IteratorStarID;
import org.rdfhdt.hdt.stars.StarID;
import org.rdfhdt.hdt.util.Tuple;

import java.util.*;

public class BitmapStarIterator implements IteratorStarID {
    private final BitmapTriplesPp triples;
    private final List<Long> families;
    private final StarID star;
    private final Map<Long, Tuple<Bitmap, Sequence>> data;
    private Map<Long, Long> ptrs;
    private Queue<StarID> queue = new LinkedList<>();

    private StarID next = null;
    private long family = 0, start = 0, end = 0, snum = 1, s = 0, sid = 0;
    private int i = 0;
    private boolean newfam = true, news = true;

    public BitmapStarIterator(BitmapTriplesPp triples, StarID star, List<Long> families, Map<Long, Tuple<Bitmap, Sequence>> map) {
        this.triples = triples;
        this.families = families;
        this.star = star;
        this.data = map;

        Map<Long, Long> ptrs = new HashMap<>();
        List<Integer> lst = star.getPredicates();
        int size = lst.size();
        for(int i = 0; i < size; i++) {
            int in = lst.get(i);
            ptrs.put((long) in, 0L);
        }
        this.ptrs = ptrs;
    }

    @Override
    public boolean hasPrevious() {
        return false;
    }

    @Override
    public StarID previous() {
        return null;
    }

    @Override
    public void goToStart() {
    }

    @Override
    public boolean canGoTo() {
        return false;
    }

    @Override
    public void goTo(long pos) {
    }

    @Override
    public long estimatedNumResults() {
        return 0;
    }

    @Override
    public ResultEstimationType numResultEstimation() {
        return null;
    }

    @Override
    public TripleComponentOrder getOrder() {
        return null;
    }

    @Override
    public boolean hasNext() {
        if(next == null)
            bufferNext();
        return next != null;
    }

    @Override
    public StarID next() {
        if (next == null)
            bufferNext();
        StarID n = next;
        next = null;
        return n;
    }

    private void bufferNext() {
        //todo eliminate recursion
        if (!queue.isEmpty()) {
            next = queue.poll();
            return;
        }

        if (i >= families.size()) {
            next = null;
            return;
        }

        if (newfam) {
            family = families.get(i);
            start = triples.seqFirst.get(family - 1);
            if (family > triples.seqF.getNumberOfElements()) {
                end = triples.permS.getNumberOfElements();
            } else {
                end = triples.seqFirst.get(family);
            }
            newfam = false;
            s = start;
            snum = 1;
            goToFamily();
        }

        if (s >= end) {
            newfam = true;
            i++;
            bufferNext();
            return;
        }

        sid = triples.getSubjIDPermS(snum, family);
        int subj = star.getSubject();
        if (subj != 0 && sid != subj) {
            snum++;
            s++;
            bufferNext();
            return;
        }

        Map<Long, List<Long>> map = new HashMap<>();
        boolean empty = false;
        List<Tuple<Integer, Integer>> ts = star.getTriples();
        int size = ts.size();
        for(int i = 0; i < size; i++) {
            Tuple<Integer, Integer> t = ts.get(i);
            long lx = t.x;
            Bitmap bmap = data.get(lx).x;
            Sequence seq = data.get(lx).y;
            long ptr = ptrs.get(lx);
            List<Long> lst = new ArrayList<>();

            do {
                if(ptr >= seq.getNumberOfElements()) break;
                long oid = triples.getObjIDPermO(seq.get(ptr), t.x);
                if(t.y == 0 || t.y == oid) {
                    lst.add(oid);
                }
                ptr++;
            } while(!bmap.access(ptr));
            ptrs.put(lx, ptr);

            if(lst.size() == 0) {
                empty = true;
                break;
            }
            map.put(lx, lst);
        }

        if(empty) {
            snum++;
            s++;
            bufferNext();
            return;
        }

        enqueue(sid, map);

        snum++;
        s++;

        bufferNext();


        /*
        while(queue.isEmpty()) {
            if (i >= families.size()) {
                next = null;
                return;
            }

            if (newfam) {
                family = families.get(i);
                start = triples.seqFirst.get(family - 1);
                if (family > triples.seqF.getNumberOfElements()) {
                    end = triples.permS.getNumberOfElements();
                } else {
                    end = triples.seqFirst.get(family);
                }
                newfam = false;
                s = start;
                snum = 1;
                goToFamily();
            }

            if (s >= end) {
                newfam = true;
                i++;
                continue;
            }

            sid = triples.getSubjIDPermS(snum, family);
            if (star.getSubject() != 0 && sid != star.getSubject()) {
                snum++;
                s++;
                continue;
            }

            Map<Long, List<Long>> map = new HashMap<>();
            boolean empty = false;
            List<Tuple<Integer, Integer>> ts = star.getTriples();
            int size = ts.size();
            for(int i = 0; i < size; i++) {
                Tuple<Integer, Integer> t = ts.get(i);
                Bitmap bmap = data.get((long)t.x).x;
                Sequence seq = data.get((long)t.x).y;
                long ptr = ptrs.get((long)t.x);
                List<Long> lst = new ArrayList<>();

                do {
                    if(ptr >= seq.getNumberOfElements()) break;
                    long oid = triples.getObjIDPermO(seq.get(ptr), t.x);
                    if(t.y == 0 || t.y == oid) {
                        lst.add(oid);
                    }
                    ptr++;
                } while(!bmap.access(ptr));
                ptrs.put((long)t.x, ptr);

                if(lst.size() == 0) {
                    empty = true;
                }
                map.put((long)t.x, lst);
            }

            if(empty) {
                snum++;
                s++;
                continue;
            }

            enqueue(sid, map);

            snum++;
            s++;
        }

        next = queue.poll();
         */
    }

    private void enqueue(long sid, Map<Long, List<Long>> map) {
        List<StarID> tmp = new ArrayList<>();

        // Find max size
        int max = 0;
        Iterator<List<Long>> it = map.values().iterator();
        while(it.hasNext()) {
            List<Long> l = it.next();
            if(l.size() > max) max = l.size();
        }

        List<Tuple<Integer, Integer>> lst = new ArrayList<>();
        Iterator<Map.Entry<Long, List<Long>>> it1 = map.entrySet().iterator();
        while(it1.hasNext()) {
            Map.Entry<Long, List<Long>> e = it1.next();
            long p = e.getKey();
            long o = e.getValue().get(0);
            lst.add(new Tuple<>((int) p, (int) o));
        }
        tmp.add(new StarID((int)sid, lst));

        for(int i = 1; i < max; i++) {
            List<StarID> tmp1 = new ArrayList<>(tmp);
            it1 = map.entrySet().iterator();
            while(it1.hasNext()) {
                Map.Entry<Long, List<Long>> e = it1.next();
                if(e.getValue().size() >= max) {
                    long p = e.getKey();
                    long o = e.getValue().get(i);
                    int sz = tmp1.size();
                    for(int j = 0; j < sz; j++) {
                        StarID starid = tmp1.get(j);
                        starid.setObject((int) p, (int) o);
                        tmp.add(starid);
                    }
                }
            }
        }

        int sz = tmp.size();
        for(int i = 0; i < sz; i++) {
            queue.offer(tmp.get(i));
        }
    }

    private void goToFamily() {
        Iterator<Map.Entry<Long, Tuple<Bitmap, Sequence>>> it = data.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Tuple<Bitmap, Sequence>> e = it.next();
            long famptr = 0;
            long famnum = 1;
            long pred = e.getKey();
            Bitmap bmap = e.getValue().x;
            long ptr = 0;

            while (famnum != family) {
                if (triples.seqF.get(famptr) == pred) {
                    long end;
                    if (famnum > triples.seqF.getNumberOfElements()) {
                        end = triples.permS.getNumberOfElements();
                    } else {
                        end = triples.seqFirst.get(famnum);
                    }
                    long count = end - triples.seqFirst.get(famnum - 1);

                    for (long i = 0; i < count; i++) {
                        while (!bmap.access(ptr)) {
                            ptr++;
                        }
                        ptr++;
                    }
                }
                if (triples.bitmapF.access(famptr)) {
                    famnum++;
                }
                famptr++;
            }

            ptrs.put(pred, ptr);
        }
    }
}
