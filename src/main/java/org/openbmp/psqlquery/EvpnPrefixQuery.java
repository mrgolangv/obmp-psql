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
import org.openbmp.api.parsed.message.EvpnPrefixPojo;

import java.util.List;


public class EvpnPrefixQuery extends Query {
    private final List<EvpnPrefixPojo> records;

	public EvpnPrefixQuery(List<EvpnPrefixPojo> records){
		
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
        String [] stmt = { " INSERT INTO evpn_rib (hash_id, peer_hash_id, path_attr_hash_id, origin_as, route_type, " +
                            "gateway, prefix, prefix_len, mac, mac_len, timestamp, iswithdrawn, path_id, mpls_label_1, mpls_label_2, " +
                            "isprepolicy, isadjribin, rd_administrator_subfield, rd_type, originating_router_ip_len, originating_router_ip, " +
                            "ethernet_tag_id_hex, ethernet_segment_identifier) " +

//                            " VALUES ",
                            "SELECT DISTINCT ON (hash_id) * FROM ( VALUES ",

                            ") t(hash_id, peer_hash_id, path_attr_hash_id, origin_as, route_type, " +
                                    "gateway, prefix, prefix_len, mac, mac_len, timestamp, iswithdrawn, path_id, mpls_label_1, mpls_label_2, " +
                                    "isprepolicy, isadjribin, rd_administrator_subfield, rd_type, originating_router_ip_len, originating_router_ip, " +
                                    "ethernet_tag_id_hex, ethernet_segment_identifier)" +
                           " ORDER BY hash_id" +
                           " ON CONFLICT (peer_hash_id,hash_id,path_attr_hash_id) DO UPDATE SET " +
                               "path_attr_hash_id=CASE excluded.isWithdrawn WHEN true THEN evpn_rib.path_attr_hash_id ELSE excluded.path_attr_hash_id END," +
                                "origin_as=CASE excluded.isWithdrawn WHEN true THEN evpn_rib.origin_as ELSE excluded.origin_as END," +
                                "mpls_label_1=CASE excluded.isWithdrawn WHEN true THEN evpn_rib.mpls_label_1 ELSE excluded.mpls_label_1 END," +
                                "mpls_label_2=CASE excluded.isWithdrawn WHEN true THEN evpn_rib.mpls_label_2 ELSE excluded.mpls_label_2 END," +
                                "rd_administrator_subfield=CASE excluded.isWithdrawn WHEN true THEN evpn_rib.rd_administrator_subfield ELSE excluded.rd_administrator_subfield END," +
                                "originating_router_ip=CASE excluded.isWithdrawn WHEN true THEN evpn_rib.originating_router_ip ELSE excluded.originating_router_ip END," +
                                "isWithdrawn=excluded.isWithdrawn," +
                                "path_id=excluded.path_id," +
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
        for (EvpnPrefixPojo pojo: records) {

            if (i > 0)
                sb.append(',');

            i++;
            sb.append("('");
            sb.append(pojo.getVpnHash()); sb.append("'::uuid,");                        //hash_id
            sb.append('\''); sb.append(pojo.getPeer_hash()); sb.append("'::uuid,");     //peer_hash_id

            if (pojo.getPath_attr_hash_id().length() != 0) {
                sb.append('\'');
                sb.append(pojo.getPath_attr_hash_id());                                 //path_attr_hash_id
                sb.append("'::uuid,");
            } else {
                sb.append("null::uuid,");
            }
            sb.append(pojo.getOrigin_asn()); sb.append(',');                            //origin_as
            sb.append(pojo.getRouteType()); sb.append(',');                             //route_type
            if (pojo.getGateway().length() != 0) {
                sb.append('\'');
                sb.append(pojo.getGateway());                                           //gateway
                sb.append("'::inet,");
            } else {
                sb.append("null::inet,");
            }
            if (pojo.getPrefix().length() != 0) {
                sb.append('\''); sb.append(pojo.getPrefix()); sb.append('/'); //prefix
                sb.append(pojo.getPrefix_len());
                sb.append("'::inet,");
            } else {
                sb.append("null::inet,");
            }
            sb.append(pojo.getPrefix_len()); sb.append(',');                            //prefix_len

            sb.append('\''); sb.append(pojo.getMac()); sb.append("',"); //prefix         //mac
            sb.append(pojo.getMac_len()); sb.append(',');                               //mac_len
            sb.append('\''); sb.append(pojo.getTimestamp()); sb.append("'::timestamp,");//ts
            sb.append(pojo.getWithdrawn()); sb.append(',');                             //isWithdrawn
            sb.append(pojo.getPath_id()); sb.append(',');                               //path_id
            sb.append(pojo.getLabel_1()); sb.append(',');                               //mpls_label_1
            sb.append(pojo.getLabel_2()); sb.append(',');                               //mpls_label_2
            sb.append(pojo.getPrePolicy());sb.append("::boolean,");                     //isPrePolicy
            sb.append(pojo.getAdjRibIn()); sb.append("::boolean,");                     //isAdjRibIn
            sb.append('\''); sb.append(pojo.getRdAdminSubfield()); sb.append("',");     //rd_admin_subfield
            sb.append(pojo.getRdType()); sb.append(',');                                //rd_type
            sb.append(pojo.getOriginRouterIpLen()); sb.append(',');                     //OriginRouterIpLen
            if (pojo.getOriginRouterIp().length() != 0) {
                sb.append('\'');
                sb.append(pojo.getOriginRouterIp());                                     //OriginRouterIp
                sb.append("'::inet,");
            } else {
                sb.append("null::inet,");
            }
            sb.append('\''); sb.append(pojo.getEthernetTagIdHex()); sb.append("',");     //EthernetTagIdHex
            sb.append('\''); sb.append(pojo.getEthernetSegId()); sb.append("'");        //EthernetSegId

            sb.append(')');

        }
        return sb.toString();
    }

}
