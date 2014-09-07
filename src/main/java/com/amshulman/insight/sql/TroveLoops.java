package com.amshulman.insight.sql;

import gnu.trove.map.TIntByteMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TIntShortMap;
import gnu.trove.procedure.TIntByteProcedure;
import gnu.trove.procedure.TIntIntProcedure;
import gnu.trove.procedure.TIntObjectProcedure;
import gnu.trove.procedure.TIntShortProcedure;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TroveLoops {

    @AllArgsConstructor(access = AccessLevel.PACKAGE, staticName = "with")
    static final class AddWhereClauseObjectParameters<T> implements TIntObjectProcedure<T> {

        private final TIntObjectMap<T> destination;
        private final int offset;

        @Override
        public boolean execute(int a, T b) {
            destination.put(a + offset - 1, b);
            return true;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PACKAGE, staticName = "with")
    static final class AddWhereClauseIntParameters implements TIntIntProcedure {

        private final TIntIntMap destination;
        private final int offset;

        @Override
        public boolean execute(int a, int b) {
            destination.put(a + offset - 1, b);
            return true;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PACKAGE, staticName = "with")
    static final class AddWhereClauseShortParameters implements TIntShortProcedure {

        private final TIntShortMap destination;
        private final int offset;

        @Override
        public boolean execute(int a, short b) {
            destination.put(a + offset - 1, b);
            return true;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PACKAGE, staticName = "with")
    static final class AddWhereClauseByteParameters implements TIntByteProcedure {

        private final TIntByteMap destination;
        private final int offset;

        @Override
        public boolean execute(int a, byte b) {
            destination.put(a + offset - 1, b);
            return true;
        }
    }
}
