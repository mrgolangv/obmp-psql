/*
 * Copyright (c) 2018-2020 Tim Evens (tim@evensweb.com).  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 *
 */
package org.openbmp.psqlquery;

import org.openbmp.api.helpers.IpAddr;
import org.openbmp.api.parsed.message.L3VpnPrefixPojo;

import java.util.List;


public class L3VpnPrefixQuery extends Query {
    private final List<L3VpnPrefixPojo> records;

	public L3VpnPrefixQuery(List<L3VpnPrefixPojo> records){
		
		this.records = records;
	}
	
	
    /**
     * Generate insert/update statement, sans the values
     *
     * @return Two strings are returned
     *      0 = Insert statement string up to VALUES keyword
     *      1 = ON DUPLICATE KEY UPDATE ...  or empty if not used.
     */
    public String[] genInsertStatement() {
        String [] stmt = { " INSERT INTO l3vpn_rib (hash_id, peer_hash_id, path_attr_hash_id, isIPv4, origin_as, prefix, prefix_len, " +
                "prefix_bin, prefix_bcast_bin, prefix_bits, timestamp, isWithdrawn, path_id, labels, isPrePolicy, isAdjRibIn, rd) " +

//                            " VALUES ",
                            "SELECT DISTINCT ON (hash_id) * FROM ( VALUES ",

                            ") t(hash_id, peer_hash_id, path_attr_hash_id, isIPv4, origin_as, prefix, prefix_len, " +
                                    "prefix_bin, prefix_bcast_bin, prefix_bits, timestamp, isWithdrawn, path_id, labels, isPrePolicy, isAdjRibIn, rd) " +
                           " ORDER BY hash_id,timestamp desc" +
                           " ON CONFLICT (peer_hash_id,hash_id) DO UPDATE SET timestamp=excluded.timestamp," +
                               "path_attr_hash_id=CASE excluded.isWithdrawn WHEN true THEN l3vpn_rib.path_attr_hash_id ELSE excluded.path_attr_hash_id END," +
                               "origin_as=CASE excluded.isWithdrawn WHEN true THEN l3vpn_rib.origin_as ELSE excluded.origin_as END," +
                               "isWithdrawn=excluded.isWithdrawn," +
                               "path_id=excluded.path_id, labels=excluded.labels," +
                               "isPrePolicy=excluded.isPrePolicy, isAdjRibIn=excluded.isAdjRibIn "
                        };
        return stmt;
    }

    /**
     * Generate bulk values statement for SQL bulk insert.
     *
     * @return String in the format of (col1, col2, ...)[,...]
     */
    public String genValuesStatement() {
        StringBuilder sb = new StringBuilder();

        int i = 0;
        for (L3VpnPrefixPojo pojo: records) {

            if (i > 0)
                sb.append(',');

            i++;

            sb.append("('");
            sb.append(pojo.getHash()); sb.append("'::uuid,"); // hash_id
            sb.append('\''); sb.append(pojo.getPeer_hash()); sb.append("'::uuid,"); // peer_hash_id

            if (pojo.getPath_attr_hash_id().length() != 0) { // // base_attr_hash_id
                sb.append('\'');
                sb.append(pojo.getPath_attr_hash_id());
                sb.append("'::uuid,");
            } else {
                sb.append("null::uuid,");
            }

            sb.append(pojo.getIPv4()); sb.append("::boolean,"); // isipv4

            sb.append(pojo.getOrigin_asn()); sb.append(','); // origin_as

            sb.append('\''); sb.append(pojo.getPrefix()); sb.append('/'); //prefix
            sb.append(pojo.getPrefix_len());
            sb.append("'::inet,");

            sb.append(pojo.getPrefix_len()); sb.append(','); // prefix_len

            try {
                sb.append('\''); sb.append(IpAddr.getIpBits(pojo.getPrefix()).substring(0, pojo.getPrefix_len())); // prefix_bits
                sb.append("',");
            } catch (StringIndexOutOfBoundsException e) {
                //TODO: Fix getIpBits to support mapped IPv4 addresses in IPv6 (::ffff:ipv4)
                System.out.println("IP prefix failed to convert to bits: " +
                        pojo.getPrefix() + " len: " + pojo.getPrefix_len());
                sb.append("'',");
            }

            try {
                sb.append('\''); sb.append(IpAddr.getIpBits(pojo.getPrefix()).substring(0, pojo.getPrefix_len())); // prefix_bits
                sb.append("',");
            } catch (StringIndexOutOfBoundsException e) {
                //TODO: Fix getIpBits to support mapped IPv4 addresses in IPv6 (::ffff:ipv4)
                System.out.println("IP prefix failed to convert to bits: " +
                        pojo.getPrefix() + " len: " + pojo.getPrefix_len());
                sb.append("'',");
            }

            try {
                sb.append('\''); sb.append(IpAddr.getIpBits(pojo.getPrefix()).substring(0, pojo.getPrefix_len())); // prefix_bits
                sb.append("',");
            } catch (StringIndexOutOfBoundsException e) {
                //TODO: Fix getIpBits to support mapped IPv4 addresses in IPv6 (::ffff:ipv4)
                System.out.println("IP prefix failed to convert to bits: " +
                        pojo.getPrefix() + " len: " + pojo.getPrefix_len());
                sb.append("'',");
            }

            sb.append('\''); sb.append(pojo.getTimestamp()); sb.append("'::timestamp,"); //ts
            sb.append(pojo.getWithdrawn()); sb.append(','); //isWithdrawn
            sb.append(pojo.getPath_id()); sb.append(','); // path_id
            sb.append('\''); sb.append(pojo.getLabels()); sb.append("',"); //labels
            sb.append(pojo.getPrePolicy()); sb.append("::boolean,"); //isPrePolicy
            sb.append(pojo.getAdjRibIn()); sb.append("::boolean,"); //isAdjRibIn
            sb.append('\''); sb.append(pojo.getRd()); sb.append("'"); // rd
            sb.append(')');
        }
        System.out.println(sb.toString());
        return sb.toString();
    }

}
