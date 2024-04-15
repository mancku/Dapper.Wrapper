namespace Dapper.Wrapper.Test.Models
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;

    [Table("SalesOrderDetail")]
    public class SalesOrderDetail
    {
        public int SalesOrderID { get; set; }

        public int SalesOrderDetailID { get; set; }

        public short OrderQty { get; set; }

        [ForeignKey(nameof(SalesOrderProduct))]
        public int ProductID { get; set; }

        public decimal UnitPrice { get; set; }

        public decimal UnitPriceDiscount { get; set; }

        public decimal LineTotal { get; set; }

        public Guid? rowguid { get; set; }

        public DateTime ModifiedDate { get; set; }

        public Product SalesOrderProduct { get; set; }
    }
}
