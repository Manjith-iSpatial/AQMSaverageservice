
namespace AQMSWindowsApp
{
    partial class ServerParameterData
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.btnSetTimeOut = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // btnSetTimeOut
            // 
            this.btnSetTimeOut.Location = new System.Drawing.Point(0, 0);
            this.btnSetTimeOut.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.btnSetTimeOut.Name = "btnSetTimeOut";
            this.btnSetTimeOut.Size = new System.Drawing.Size(291, 59);
            this.btnSetTimeOut.TabIndex = 0;
            this.btnSetTimeOut.Text = "Calculate Averages";
            this.btnSetTimeOut.UseVisualStyleBackColor = true;
            this.btnSetTimeOut.Click += new System.EventHandler(this.btnSetTimeOut_Click);
            // 
            // ServerParameterData
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.btnSetTimeOut);
            this.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Name = "ServerParameterData";
            this.Text = "AQMS Server Average Service";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button btnSetTimeOut;
    }
}

